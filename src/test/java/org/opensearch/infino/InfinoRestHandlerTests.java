/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

 package org.opensearch.infino;

import org.junit.Before;
import org.junit.After;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.AbstractRestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.test.OpenSearchTestCase;
// import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.PushPromiseHandler;
import java.net.http.HttpHeaders;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * Test the Infino Rest handler.
 *
 * We have to manually mock a number of classes due to conflicts between HttpRequest
 * class in OpenSearch and Java and the way threads are handled in OpenSearch tests.
 *
 * TODO: There must be a cleaner way to write this.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class InfinoRestHandlerTests extends OpenSearchTestCase {
    private NodeClient mockNodeClient;
    private ExecutorService executorService;
    private InfinoSerializeRequestURI mockInfinoSerializeRequestURI;
    private InfinoRestHandler handler;
    private List<CompletableFuture<?>> futures;

    private static final Logger logger = LogManager.getLogger(InfinoRestHandlerTests.class);

    private MyHttpClient mockMyHttpClient = new MyHttpClient() {
        @Override
        public CompletableFuture<HttpResponse<String>> sendAsyncRequest(HttpRequest request, HttpResponse.BodyHandler<String> responseBodyHandler) {
            // Return a CompletableFuture with the mocked response
            CompletableFuture<HttpResponse<String>> future = new CompletableFuture<>();
            future.complete(createFakeResponse());
            futures.add(future);
            return future;
        }
    };

    // Define an interface to wrap HttpClient calls
    public interface MyHttpClient {
        CompletableFuture<HttpResponse<String>> sendAsyncRequest(HttpRequest request, HttpResponse.BodyHandler<String> responseBodyHandler);
    }

    // Use a single thread for testing
    // Mock the client and URI serializer
    // Track futures so we can track thread leaks (might not be necessary)
    @Before
    public void setUp() throws Exception {
        super.setUp();
        executorService = Executors.newSingleThreadExecutor();
        mockNodeClient = mock(NodeClient.class);
        mockInfinoSerializeRequestURI = mock(InfinoSerializeRequestURI.class);
        futures = new ArrayList<>(); // Initialize the list to track futures

        // Override key methods in the handler
        handler = new InfinoRestHandler() {
            @Override
            protected ExecutorService getInfinoThreadPool() {
                return executorService;
            }

            @Override
            protected InfinoSerializeRequestURI getInfinoSerializeRequestURI(RestRequest request) {
                return mockInfinoSerializeRequestURI;
            }

            @Override
            protected HttpClient getHttpClient() {
                return getCustomHttpClient();
            }

            @Override
            public String getName() {
                return "test_rest_handler_infino";
            }
        };
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        executorService.shutdown();
        // Await termination with a timeout to ensure tests complete promptly
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException ie) {
            executorService.shutdownNow();
        }

        // Wait for all futures to complete
        CompletableFuture<Void> allDone = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        try {
            allDone.get(5, TimeUnit.SECONDS); // Adjust the timeout as needed
        } catch (Exception e) {
            e.printStackTrace(); // Consider proper logging
        }
        // Clear the list of futures
        futures.clear();
    }

    // Create a fake HttpResponse for testing
    private HttpResponse<String> createFakeResponse() {
        return new HttpResponse<>() {
            @Override
            public int statusCode() {
                return 200; // OK status
            }

            @Override
            public Optional<HttpResponse<String>> previousResponse() {
                return Optional.empty();
            }

            @Override
            public HttpRequest request() {
                return HttpRequest.newBuilder().uri(URI.create("http://example.com")).build();
            }

            @Override
            public HttpHeaders headers() {
                return HttpHeaders.of(new HashMap<>(), (s, s2) -> true);
            }

            @Override
            public String body() {
                return "hello world"; // Fake body content
            }

            @Override
            public Optional<SSLSession> sslSession() {
                return Optional.empty();
            }

            @Override
            public URI uri() {
                return request().uri();
            }

            @Override
            public HttpClient.Version version() {
                return HttpClient.Version.HTTP_1_1;
            }
        };
    }

    private HttpClient getCustomHttpClient () {
        return new HttpClient() {
            @Override
            public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request, BodyHandler<T> responseBodyHandler, PushPromiseHandler<T> pushPromiseHandler) {
                throw new UnsupportedOperationException("Not implemented in mock");
            }

            @Override
            public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request, BodyHandler<T> responseBodyHandler) {
                CompletableFuture<HttpResponse<String>> future = mockMyHttpClient.sendAsyncRequest(request, convertToSpecificHandler(responseBodyHandler));
                return future.thenApply(response -> convertToGenericResponse(response, responseBodyHandler));
            }

            // Helper method to convert BodyHandler<T> to BodyHandler<String>
            private BodyHandler<String> convertToSpecificHandler(BodyHandler<?> handler) {
                return HttpResponse.BodyHandlers.ofString();
            }

            @Override
            public Optional<CookieHandler> cookieHandler() {
                return Optional.empty();
            }

            @Override
            public Optional<Duration> connectTimeout() {
                return Optional.empty();
            }

            @Override
            public Redirect followRedirects() {
                return null;
            }

            @Override
            public Optional<ProxySelector> proxy() {
                return Optional.empty();
            }

            @Override
            public SSLContext sslContext() {
                return null;
            }

            @Override
            public SSLParameters sslParameters() {
                return null;
            }

            @Override
            public Optional<Authenticator> authenticator() {
                return Optional.empty();
            }

            @Override
            public Version version() {
                return Version.HTTP_1_1;
            }

            @Override
            public Optional<Executor> executor() {
                return Optional.empty();
            }

            @Override
            public <T> HttpResponse<T> send(HttpRequest request, BodyHandler<T> responseBodyHandler)
                    throws IOException, InterruptedException {
                throw new UnsupportedOperationException("Unimplemented method 'send'");
            }
        };
    }

    // Helper method to convert HttpResponse<String> to HttpResponse<T>
    private <T> HttpResponse<T> convertToGenericResponse(HttpResponse<String> response, BodyHandler<T> handler) {
        @SuppressWarnings("unchecked")
        HttpResponse<T> genericResponse = (HttpResponse<T>) response;
        return genericResponse;
    }

    // We use our own FakeRestChannel (from BaseRestHandler tests) because we need to
    // access latch to wait on thread sync in the handler.
    public final class FakeRestChannel extends AbstractRestChannel {
        protected final CountDownLatch latch;
        private final AtomicInteger responses = new AtomicInteger();
        private final AtomicInteger errors = new AtomicInteger();
        private RestResponse capturedRestResponse;

        public FakeRestChannel(RestRequest request, boolean detailedErrorsEnabled, int responseCount) {
            super(request, detailedErrorsEnabled);
            this.latch = new CountDownLatch(responseCount);
        }

        @Override
        public void sendResponse(RestResponse response) {
            this.capturedRestResponse = response;
            if (response.status() == RestStatus.OK) {
                responses.incrementAndGet();
            } else {
                errors.incrementAndGet();
            }
            latch.countDown();
        }

        public RestResponse capturedResponse() {
            return capturedRestResponse;
        }

        public AtomicInteger responses() {
            return responses;
        }

        public AtomicInteger errors() {
            return errors;
        }
    }

    // Test successful GET request
    public void testGetRequest() throws Exception {
        testRequestWithMethod(RestRequest.Method.GET, "hello world");
    }

    // Test successful POST request
    public void testPostRequest() throws Exception {
        testRequestWithMethod(RestRequest.Method.POST, "hello world");
    }

    // Test handling of a non-existent endpoint
    public void testNonExistentEndpoint() throws Exception {
        testRequestWithMethod(RestRequest.Method.GET, "Not Found", RestStatus.NOT_FOUND, "/non-existent-endpoint");
    }

    // Test handling of server error (e.g., Infino service down)
    public void testServerError() throws Exception {
        String path = "/infino/test-index/_ping?invalidParam=value";
        when(mockMyHttpClient.sendAsyncRequest(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(createClientErrorResponse()));
        testRequestWithMethod(RestRequest.Method.GET, "Internal Server Error", RestStatus.INTERNAL_SERVER_ERROR, path);
    }

    // Helper method to test requests with a specific method and expected response
    private void testRequestWithMethod(RestRequest.Method method, String expectedBody) throws Exception {
        testRequestWithMethod(method, expectedBody, RestStatus.OK, "/infino/test-index/_ping");
    }

    // Test successful PUT request
    public void testPutRequest() throws Exception {
        testRequestWithMethod(RestRequest.Method.PUT, "Resource updated successfully");
    }

    // Test successful DELETE request
    public void testDeleteRequest() throws Exception {
        testRequestWithMethod(RestRequest.Method.DELETE, "Resource deleted successfully");
    }

    // Test request with invalid parameters
    public void testInvalidParameters() throws Exception {
        String path = "/infino/test-index/_ping?invalidParam=value";
        testRequestWithMethod(RestRequest.Method.GET, "Invalid parameters", RestStatus.BAD_REQUEST, path);
    }

    // Test handling of client error (e.g., bad request)
    public void testClientError() throws Exception {
        String path = "/infino/test-index/_ping?invalidParam=value";
        when(mockMyHttpClient.sendAsyncRequest(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(createClientErrorResponse()));
        testRequestWithMethod(RestRequest.Method.GET, "Bad Request", RestStatus.BAD_REQUEST, path);
    }

    // Test handling of request timeouts
    public void testRequestTimeout() throws Exception {
        String path = "/infino/test-index/_ping?invalidParam=value";
        // Simulate a timeout scenario
        CompletableFuture<HttpResponse<String>> delayedFuture = new CompletableFuture<>();
        when(mockMyHttpClient.sendAsyncRequest(any(), any())).thenReturn(delayedFuture);
        testRequestWithMethod(RestRequest.Method.GET, "Request timeout", RestStatus.REQUEST_TIMEOUT, path);

        // Complete the future after a deliberate delay
        Executors.newSingleThreadScheduledExecutor().schedule(
                () -> delayedFuture.complete(createFakeResponse()), 10, TimeUnit.SECONDS);
    }

    // Test response with a large payload
    public void testLargeResponsePayload() throws Exception {
        String largePayload = String.join("", Collections.nCopies(1000, "Large payload. "));
        when(mockMyHttpClient.sendAsyncRequest(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(createCustomResponse(largePayload)));
        testRequestWithMethod(RestRequest.Method.GET, largePayload);
    }

    // Test handling when Infino service is unavailable (e.g., network issue)
    public void testInfinoServiceUnavailable() throws Exception {
        String path = "/infino/test-index/_ping?invalidParam=value";
        when(mockMyHttpClient.sendAsyncRequest(any(), any()))
                .thenReturn(CompletableFuture.failedFuture(new IOException("Service Unavailable")));
        testRequestWithMethod(RestRequest.Method.GET, "Service Unavailable", RestStatus.SERVICE_UNAVAILABLE, path);
    }

    private class ErrorResponse extends HttpResponse
    // Create a fake client error response for testing
    private HttpResponse<String> createClientErrorResponse() {
        return new HttpResponse<>() {
            @Override
            public int statusCode() {
                return 500; // Server Error
            }
        };
    }

    // Create a custom response with a specified body for testing
    private HttpResponse<String> createCustomResponse(String responseBody) {
        return new HttpResponse<>() {
            // ... [override methods as necessary, return responseBody for the body] ...
        };
    }

    // Test that the REST request is serialized correctly
    public void testRequestSerialization() throws Exception {
        String expectedUri = "http://test-host:3000/infino/ping";
        when(mockInfinoSerializeRequestURI.getFinalUrl()).thenReturn(expectedUri);
        when(mockInfinoSerializeRequestURI.getMethod()).thenReturn(RestRequest.Method.GET);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/infino/test-index/_ping").build();
        FakeRestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);

        handler.handleRequest(request, channel, mockNodeClient);
        assertTrue("Request serialization did not complete", channel.latch.await(5, TimeUnit.SECONDS));
        assertEquals("Expected final URI did not match", expectedUri, mockInfinoSerializeRequestURI.getFinalUrl());
    }

    // Test handling when Infino returns a non-OK response
    public void testNonOkResponseFromInfino() throws Exception {
        simulateInfinoResponse(RestStatus.INTERNAL_SERVER_ERROR, "Internal Server Error");
        testRequestResponse(RestStatus.INTERNAL_SERVER_ERROR, "Internal Server Error");
    }

    // Test handling when an exception occurs during request processing
    public void testExceptionDuringRequestProcessing() throws Exception {
        simulateInfinoResponseException(new IOException("Network error"));
        testRequestResponse(RestStatus.SERVICE_UNAVAILABLE, "Network error");
    }

    // Helper method to simulate a response from Infino
    private void simulateInfinoResponse(RestStatus status, String responseBody) {
        when(mockMyHttpClient.sendAsyncRequest(any(HttpRequest.class), any()))
            .thenReturn(CompletableFuture.completedFuture(createFakeResponse(status, responseBody)));
    }

    // Helper method to simulate an exception during a response from Infino
    private void simulateInfinoResponseException(Exception exception) {
        CompletableFuture<HttpResponse<String>> future = new CompletableFuture<>();
        future.completeExceptionally(exception);
        when(mockMyHttpClient.sendAsyncRequest(any(HttpRequest.class), any())).thenReturn(future);
    }

    // Helper method to create a fake HttpResponse for testing with custom status and body
    private HttpResponse<String> createFakeResponse(RestStatus status, String body) {
        return new HttpResponse<>() {
            @Override
            public int statusCode() {
                return status.getStatus();
            }

            // ... [other overridden methods] ...

            @Override
            public String body() {
                return body;
            }
        };
    }

    // Helper method to test the response from a request
    private void testRequestResponse(RestStatus expectedStatus, String expectedBody) throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/infino/test-index/_action").build();
        FakeRestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);

        handler.handleRequest(request, channel, mockNodeClient);

        assertTrue("Response was not received in time", channel.latch.await(5, TimeUnit.SECONDS));
        assertEquals("Unexpected response status", expectedStatus, channel.capturedResponse().status());
        assertEquals("Unexpected response body", expectedBody, channel.capturedResponse().content().utf8ToString());
    }
    // Test handling when Lucene index already exists
    public void testLuceneIndexExists() throws Exception {
        simulateIndexExists("test-index", true);
        testRequestWithIndexCreation("test-index", false); // Expect no index creation
    }

    // Test creating a new Lucene index
    public void testCreateNewLuceneIndex() throws Exception {
        simulateIndexExists("new-index", false);
        testRequestWithIndexCreation("new-index", true); // Expect index creation
    }

    // Test failure when creating a Lucene index
    public void testLuceneIndexCreationFailure() throws Exception {
        simulateIndexExists("failed-index", false);
        simulateIndexCreationFailure("failed-index");
        testRequestWithIndexCreation("failed-index", true, false); // Expect index creation attempt but failure
    }

    // Simulate whether an index exists or not
    private void simulateIndexExists(String indexName, boolean exists) {
        when(mockNodeClient.admin().indices().exists(any(IndicesExistsRequest.class), any()))
            .thenAnswer(invocation -> {
                ActionListener<IndicesExistsResponse> listener = invocation.getArgument(1);
                listener.onResponse(new IndicesExistsResponse(exists));
                return null;
            });
    }

    // Simulate successful or failed index creation
    private void simulateIndexCreation(String indexName, boolean success) {
        when(mockNodeClient.admin().indices().create(any(CreateIndexRequest.class), any()))
            .thenAnswer(invocation -> {
                ActionListener<CreateIndexResponse> listener = invocation.getArgument(1);
                if (success) {
                    listener.onResponse(new CreateIndexResponse(true, true, indexName));
                } else {
                    listener.onFailure(new IOException("Failed to create index"));
                }
                return null;
            });
    }

    // Test request handling with potential index creation
    private void testRequestWithIndexCreation(String indexName, boolean expectCreation) throws Exception {
        testRequestWithIndexCreation(indexName, expectCreation, true);
    }

    // Test request handling with potential index creation and handle creation success/failure
    private void testRequestWithIndexCreation(String indexName, boolean expectCreation, boolean creationSuccess) throws Exception {
        simulateIndexCreation(indexName, creationSuccess);
        String path = "/infino/" + indexName + "/_action";
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(path).build();
        FakeRestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);

        handler.handleRequest(request, channel, mockNodeClient);

        // Wait for the response
        boolean responseReceived = channel.latch.await(5, TimeUnit.SECONDS);
        assertTrue("Response was not received in time", responseReceived);

        // Assertions based on whether an index was expected to be created or not
        if (expectCreation) {
            if (creationSuccess) {
                // Assert successful index creation and OK response
                assertEquals("Expected one response", 1, channel.responses().get());
                assertEquals("Expected status to be OK", RestStatus.OK, channel.capturedResponse().status());
            } else {
                // Assert failed index creation and error response
                assertEquals("Expected one error", 1, channel.errors().get());
                assertNotEquals("Expected status not to be OK", RestStatus.OK, channel.capturedResponse().status());
            }
        } else {
            // Assert no index creation and OK response
            assertEquals("Expected one response", 1, channel.responses().get());
            assertEquals("Expected status to be OK", RestStatus.OK, channel.capturedResponse().status());
        }
    }
    
    // Generic helper method to test requests
    private void testRequestWithMethod(RestRequest.Method method, String expectedBody, RestStatus expectedStatus, String path) throws Exception {
        when(mockInfinoSerializeRequestURI.getMethod()).thenReturn(method);
        when(mockInfinoSerializeRequestURI.getFinalUrl()).thenReturn("http://test-host:3000" + path);

        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(path).withMethod(method).build();
        FakeRestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);

        handler.handleRequest(request, channel, mockNodeClient);

        boolean responseReceived = channel.latch.await(5, TimeUnit.SECONDS);
        assertTrue("Response was not received in time", responseReceived);

        assertEquals("Expected no errors", 0, channel.errors().get());
        assertEquals("Expected one response", 1, channel.responses().get());
        assertEquals("Expected status to match", expectedStatus, channel.capturedResponse().status());
        assertEquals("Response content did not match", expectedBody, channel.capturedResponse().content().utf8ToString());
    }
}
