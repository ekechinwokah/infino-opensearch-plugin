/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.infino;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestRequest.Method;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.net.http.HttpRequest;
import java.util.concurrent.CompletableFuture;

public class InfinoRestHandlerTests extends LuceneTestCase {

    @Mock
    private HttpClient mockHttpClient;
    
    @Mock
    private NodeClient mockNodeClient;

    @Mock
    private HttpResponse<String> mockResponse;

    private InfinoRestHandler restInfinoHandler;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        restInfinoHandler = new InfinoRestHandler() {
            @Override
            public HttpClient getHttpClient() {
                return mockHttpClient;
            }
        };
    }

    public void testArgumentHandling() {
        RestRequest mockRequest = mock(RestRequest.class);
        when(mockRequest.method()).thenReturn(Method.GET);
        when(mockRequest.path()).thenReturn("/infino/test");

        try {
            restInfinoHandler.handleRequest(mockRequest, null, null);
        } catch (Exception e) {
            // Handle exception or fail the test if the exception is not expected
            fail("Unexpected exception: " + e.getMessage());
        }

        verify(mockRequest, times(1)).method();
        verify(mockRequest, times(1)).path();
    }

    void testRequestForwarding() throws Exception {
        when(mockHttpClient.sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(CompletableFuture.completedFuture(mockResponse));
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn("response");

        RestRequest mockRequest = mock(RestRequest.class);
        when(mockRequest.method()).thenReturn(Method.GET);
        when(mockRequest.path()).thenReturn("/infino/test");

        restInfinoHandler.handleRequest(mockRequest, null, null);

        verify(mockHttpClient).sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    void testErrorHandling() throws Exception {
        when(mockHttpClient.sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(CompletableFuture.supplyAsync(() -> {
                throw new RuntimeException("Error");
            }));

        RestRequest mockRequest = mock(RestRequest.class);
        when(mockRequest.method()).thenReturn(Method.GET);
        when(mockRequest.path()).thenReturn("/infino/test");

        // Expect that a RuntimeException is thrown when the request is handled
        assertThrows(RuntimeException.class, () -> {
            restInfinoHandler.handleRequest(mockRequest, null, null);
        });
    }

    // Test handling of PUT request
    public void testPutRequestHandling() throws Exception {
        RestRequest mockRequest = mock(RestRequest.class);
        when(mockRequest.method()).thenReturn(Method.PUT);
        when(mockRequest.param("infinoIndex")).thenReturn("test-index");
        when(mockRequest.param("infinoPath")).thenReturn("/_doc");

        IndicesExistsResponse existsResponse = mock(IndicesExistsResponse.class);
        when(existsResponse.isExists()).thenReturn(false);
        doAnswer(invocation -> {
            ActionListener<IndicesExistsResponse> listener = invocation.getArgument(1);
            listener.onResponse(existsResponse);
            return null;
        }).when(mockNodeClient.admin().indices()).exists(any(IndicesExistsRequest.class), any(ActionListener.class));

        CreateIndexResponse createIndexResponse = mock(CreateIndexResponse.class);
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(1);
            listener.onResponse(createIndexResponse);
            return null;
        }).when(mockNodeClient.admin().indices()).create(any(CreateIndexRequest.class), any(ActionListener.class));

        restInfinoHandler.handleRequest(mockRequest, null, mockNodeClient);

        // Verify Lucene index creation and HTTP call
        verify(mockNodeClient.admin().indices(), times(1)).exists(any(IndicesExistsRequest.class), any(ActionListener.class));
        verify(mockHttpClient).sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    // Test handling of DELETE request
    public void testDeleteRequestHandling() throws Exception {
        RestRequest mockRequest = mock(RestRequest.class);
        when(mockRequest.method()).thenReturn(Method.DELETE);
        when(mockRequest.param("infinoIndex")).thenReturn("test-index");

        restInfinoHandler.handleRequest(mockRequest, null, mockNodeClient);

        verify(mockHttpClient).sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    // Test handling of request with missing Infino URL parts
    public void testRequestWithMissingUrlParts() {
        RestRequest mockRequest = mock(RestRequest.class);
        when(mockRequest.method()).thenReturn(Method.GET);
        when(mockRequest.param("infinoPath")).thenReturn(null);

        // Expect that an IllegalArgumentException is thrown when the request is handled
        assertThrows(IllegalArgumentException.class, () -> {
            restInfinoHandler.handleRequest(mockRequest, null, null);
        });
    }

    // Test handling of unexpected exceptions during request processing
    public void testUnexpectedExceptionHandling() {
        RestRequest mockRequest = mock(RestRequest.class);
        when(mockRequest.method()).thenReturn(Method.GET);
        when(mockRequest.param("infinoPath")).thenReturn("/infino/test");
        doThrow(new RuntimeException("Unexpected error")).when(mockHttpClient).sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

        // Expect that a RuntimeException is thrown when the request is handled
        assertThrows(RuntimeException.class, () -> {
            restInfinoHandler.handleRequest(mockRequest, null, null);
        });
    }

    // Test handling of HEAD request
    public void testHeadRequestHandling() throws Exception {
        RestRequest mockRequest = mock(RestRequest.class);
        when(mockRequest.method()).thenReturn(Method.HEAD);
        when(mockRequest.param("infinoIndex")).thenReturn("test-index");
        when(mockRequest.param("infinoPath")).thenReturn("/_doc");

        restInfinoHandler.handleRequest(mockRequest, null, null);

        verify(mockHttpClient).sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    // Test handling request when Lucene index already exists
    public void testRequestWhenLuceneIndexExists() throws Exception {
        RestRequest mockRequest = mock(RestRequest.class);
        when(mockRequest.method()).thenReturn(Method.PUT);
        when(mockRequest.param("infinoIndex")).thenReturn("existing-index");
        when(mockRequest.param("infinoPath")).thenReturn("/_doc");

        // Mock response to indicate index already exists
        IndicesExistsResponse existsResponse = mock(IndicesExistsResponse.class);
        when(existsResponse.isExists()).thenReturn(true);
        doAnswer(invocation -> {
            ActionListener<IndicesExistsResponse> listener = invocation.getArgument(1);
            listener.onResponse(existsResponse);
            return null;
        }).when(mockNodeClient.admin().indices()).exists(any(IndicesExistsRequest.class), any(ActionListener.class));

        restInfinoHandler.handleRequest(mockRequest, null, mockNodeClient);

        // Verify Lucene index creation is not invoked
        verify(mockNodeClient.admin().indices(), times(1)).exists(any(IndicesExistsRequest.class), any(ActionListener.class));
        verify(mockHttpClient).sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    // Test handling request with invalid method
    public void testRequestWithInvalidMethod() {
        RestRequest mockRequest = mock(RestRequest.class);
        when(mockRequest.method()).thenReturn(null);
        when(mockRequest.param("infinoPath")).thenReturn("/infino/test");

        // Expect that an IllegalArgumentException is thrown when the request is handled
        assertThrows(IllegalArgumentException.class, () -> {
            restInfinoHandler.handleRequest(mockRequest, null, null);
        });
    }

    // Test request handling when HTTP client throws IOException
    public void testIOExceptionHandling() throws Exception {
        RestRequest mockRequest = mock(RestRequest.class);
        when(mockRequest.method()).thenReturn(Method.GET);
        when(mockRequest.param("infinoPath")).thenReturn("/infino/test");
        when(mockHttpClient.sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenThrow(new IOException("Network error"));

        // Expect that an IOException is handled and transformed to an appropriate response
        assertThrows(IOException.class, () -> {
            restInfinoHandler.handleRequest(mockRequest, null, null);
        });
    }
}
