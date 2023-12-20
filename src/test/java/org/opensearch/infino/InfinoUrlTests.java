/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.infino;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.rest.RestRequest;

public class InfinoUrlTests extends LuceneTestCase {

    @Mock
    private RestRequest mockRequest;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
    }

    void testRequestPathParsing() {
        when(mockRequest.method()).thenReturn(RestRequest.Method.GET);
        when(mockRequest.param("infinoPath")).thenReturn("/infino/logs/my-index/_search");
        when(mockRequest.param("infinoIndex")).thenReturn("my-index");

        InfinoUrl infinoUrl = new InfinoUrl(mockRequest);

        assertEquals("/infino/logs/", infinoUrl.prefix);
        assertEquals("_search", infinoUrl.path);
        assertEquals("my-index", infinoUrl.indexName);
        assertEquals(InfinoUrl.InfinoIndexType.LOGS, infinoUrl.indexType);
    }

    void testDefaultTimeRange() {
        when(mockRequest.method()).thenReturn(RestRequest.Method.GET);
        when(mockRequest.param("infinoPath")).thenReturn("/infino/logs/my-index/_search");

        InfinoUrl infinoUrl = new InfinoUrl(mockRequest);

        assertTrue(infinoUrl.params.containsKey("start_time"));
        assertTrue(infinoUrl.params.containsKey("end_time"));
    }

    void testUrlBuildingForGetMethod() {
        when(mockRequest.method()).thenReturn(RestRequest.Method.GET);
        when(mockRequest.param("infinoPath")).thenReturn("/infino/logs/my-index/_search");
        when(mockRequest.param("infinoIndex")).thenReturn("my-index");

        InfinoUrl infinoUrl = new InfinoUrl(mockRequest);

        String expectedUrl = "http://localhost:3000/my-index/search_logs?text=&start_time=<defaultStartTime>&end_time=<defaultEndTime>";
        assertEquals(expectedUrl, infinoUrl.getInfinoUrl());
    }

    void testEncodingOfUrlParameters() {
        when(mockRequest.method()).thenReturn(RestRequest.Method.GET);
        when(mockRequest.param("infinoPath")).thenReturn("/infino/logs/my-index/_search");
        when(mockRequest.param("infinoIndex")).thenReturn("my-index");
        when(mockRequest.param("text")).thenReturn("special characters &%");

        InfinoUrl infinoUrl = new InfinoUrl(mockRequest);

        assertTrue(infinoUrl.getInfinoUrl().contains("special+characters+%26%25"));
    }

    void testErrorHandlingForUnsupportedMethod() {
        when(mockRequest.method()).thenReturn(null);
        when(mockRequest.param("infinoPath")).thenReturn("/infino/logs/my-index/_search");

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new InfinoUrl(mockRequest);
        });

        String expectedMessage = "Error constructing Infino URL";
        assertTrue(exception.getMessage().contains(expectedMessage));
    }

    void testParameterHandling() {
        when(mockRequest.method()).thenReturn(RestRequest.Method.GET);
        when(mockRequest.param("infinoPath")).thenReturn("/infino/logs/my-index/_search");
        when(mockRequest.param("text")).thenReturn("query text");

        InfinoUrl infinoUrl = new InfinoUrl(mockRequest);

        assertEquals("query text", infinoUrl.params.get("text"));
    }

    void testGetMethodWithAdditionalParameters() {
        when(mockRequest.method()).thenReturn(RestRequest.Method.GET);
        when(mockRequest.param("infinoPath")).thenReturn("/infino/logs/my-index/_search");
        when(mockRequest.param("text")).thenReturn("query text");

        InfinoUrl infinoUrl = new InfinoUrl(mockRequest);

        assertTrue(infinoUrl.getInfinoUrl().contains("text=query+text"));
    }

    void testPostPutDeleteMethodUrlBuilding() {
        // Setup for POST method
        when(mockRequest.method()).thenReturn(RestRequest.Method.POST);
        when(mockRequest.param("infinoPath")).thenReturn("/infino/logs/my-index/_doc");

        InfinoUrl infinoUrl = new InfinoUrl(mockRequest);
        assertEquals("http://localhost:3000/my-index/append_log", infinoUrl.getInfinoUrl());

        // Setup for PUT method - the extra doc will get rejected by Infino
        when(mockRequest.method()).thenReturn(RestRequest.Method.PUT);
        when(mockRequest.param("infinoPath")).thenReturn("/infino/logs/my-index/_doc");

        infinoUrl = new InfinoUrl(mockRequest);
        assertEquals("http://localhost:3000/:my-index", infinoUrl.getInfinoUrl());

        // Setup for DELETE method
        when(mockRequest.method()).thenReturn(RestRequest.Method.DELETE);
        when(mockRequest.param("infinoPath")).thenReturn("/infino/logs/my-index/_doc");

        infinoUrl = new InfinoUrl(mockRequest);
        assertEquals("http://localhost:3000/my-index", infinoUrl.getInfinoUrl());
    }

    void testHandlingUndefinedIndexType() {
        when(mockRequest.method()).thenReturn(RestRequest.Method.GET);
        when(mockRequest.param("infinoPath")).thenReturn("/infino/unknown/my-index/_search");

        InfinoUrl infinoUrl = new InfinoUrl(mockRequest);

        assertEquals(InfinoUrl.InfinoIndexType.UNDEFINED, infinoUrl.indexType);
        assertNull(infinoUrl.getInfinoUrl()); // Assuming the method returns null for undefined types
    }

    void testHandlingMissingEndpointEnvVariable() {
        InfinoUrl infinoUrl = Mockito.spy(new InfinoUrl(mockRequest));
        Mockito.doReturn(null).when(infinoUrl).getEnvVariable("INFINO_SERVER_URL");

        when(mockRequest.method()).thenReturn(RestRequest.Method.GET);
        when(mockRequest.param("infinoPath")).thenReturn("/infino/logs/my-index/_search");

        assertEquals(InfinoUrl.defaultInfinoEndpoint, infinoUrl.infinoEndpoint);
    }

}

