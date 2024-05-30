// Copyright 2024 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.es.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.janusgraph.diskstorage.es.ElasticSearchMutation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class RestClientRetryTest {
    @Mock
    private RestClient restClientMock;

    @Mock
    private ResponseException responseException;

    @Mock
    private Response response;

    @Mock
    private StatusLine statusLine;

    @Captor
    private ArgumentCaptor<Request> requestCaptor;

    RestElasticSearchClient createClient(int retryAttemptLimit, Set<Integer> retryErrorCodes) throws IOException {
        //Just throw an exception when there's an attempt to look up the ES version during instantiation
        when(restClientMock.performRequest(any())).thenThrow(new IOException());

        RestElasticSearchClient clientUnderTest = new RestElasticSearchClient(restClientMock, 0, false,
            retryAttemptLimit, retryErrorCodes, 0, 0, 100_000_000);
        //There's an initial query to get the ES version we need to accommodate, and then reset for the actual test
        Mockito.reset(restClientMock);
        return clientUnderTest;
    }

    @Test
    public void testRetryOfIndividuallyFailedBulkItems() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        int retryErrorCode = 429;
        //A bulk response will still return a success despite an underlying item having an error
        when(statusLine.getStatusCode()).thenReturn(200);

        //The initial bulk request will have one element that failed in its response
        RestBulkResponse.RestBulkItemResponse initialRequestFailedItem = new RestBulkResponse.RestBulkItemResponse();
        initialRequestFailedItem.setError("An Error");
        initialRequestFailedItem.setStatus(retryErrorCode);
        RestBulkResponse initialBulkResponse = new RestBulkResponse();
        initialBulkResponse.setItems(
            Stream.of(
                Collections.singletonMap("index", new RestBulkResponse.RestBulkItemResponse()),
                Collections.singletonMap("index", initialRequestFailedItem),
                Collections.singletonMap("index", new RestBulkResponse.RestBulkItemResponse())
            ).collect(Collectors.toList())
        );
        HttpEntity initialHttpEntityMock = mock(HttpEntity.class);
        when(initialHttpEntityMock.getContent()).thenReturn(new ByteArrayInputStream(
            mapper.writeValueAsBytes(initialBulkResponse)));
        Response initialResponseMock = mock(Response.class);
        when(initialResponseMock.getEntity()).thenReturn(initialHttpEntityMock);
        when(initialResponseMock.getStatusLine()).thenReturn(statusLine);

        //The retry should then only have a single item that succeeded
        RestBulkResponse retriedBulkResponse = new RestBulkResponse();
        retriedBulkResponse.setItems(
            Collections.singletonList(Collections.singletonMap("index", new RestBulkResponse.RestBulkItemResponse())));
        HttpEntity retriedHttpEntityMock = mock(HttpEntity.class);
        when(retriedHttpEntityMock.getContent()).thenReturn(new ByteArrayInputStream(
            mapper.writeValueAsBytes(retriedBulkResponse)));
        Response retriedResponseMock = mock(Response.class);
        when(retriedResponseMock.getEntity()).thenReturn(retriedHttpEntityMock);
        when(retriedResponseMock.getStatusLine()).thenReturn(statusLine);

        try (RestElasticSearchClient restClientUnderTest = createClient(1, Sets.newHashSet(retryErrorCode))) {
            //prime the restClientMock again after it's reset after creation
            when(restClientMock.performRequest(any())).thenReturn(initialResponseMock).thenReturn(retriedResponseMock);
            restClientUnderTest.bulkRequest(Arrays.asList(
                ElasticSearchMutation.createDeleteRequest("some_index", "some_type", "some_doc_id1"),
                ElasticSearchMutation.createDeleteRequest("some_index", "some_type", "some_doc_id2"),
                ElasticSearchMutation.createDeleteRequest("some_index", "some_type", "some_doc_id3")
            ), null);
            //Verify that despite only calling bulkRequest once, we had 2 calls to the underlying rest client's
            //perform request (due to the retried failure)
            verify(restClientMock, times(2)).performRequest(requestCaptor.capture());
        }
    }

    @Test
    public void testRetryOnConfiguredErrorStatus() throws IOException {
        Integer retryCode = 429;
        int expectedNumberOfRequestAttempts = 2;
        doReturn(retryCode).when(statusLine).getStatusCode();
        doReturn(statusLine).when(response).getStatusLine();
        doReturn(response).when(responseException).getResponse();
        //Just throw an expected exception the second time to confirm the retry occurred
        //rather than mock out a parsable response
        IOException expectedFinalException = new IOException("Expected");

        try (RestElasticSearchClient restClientUnderTest = createClient(expectedNumberOfRequestAttempts - 1,
            Sets.newHashSet(retryCode))) {
            //prime the restClientMock again after it's reset after creation
            when(restClientMock.performRequest(any()))
                .thenThrow(responseException)
                .thenThrow(expectedFinalException);
            restClientUnderTest.bulkRequest(Collections.singletonList(
                ElasticSearchMutation.createDeleteRequest("some_index", "some_type", "some_doc_id")),
                null);
            Assertions.fail("Should have thrown the expected exception after retry");
        } catch (Exception actualException) {
            Assertions.assertSame(expectedFinalException, actualException);
        }
        verify(restClientMock, times(expectedNumberOfRequestAttempts)).performRequest(requestCaptor.capture());
    }

    @Test
    public void testRetriesExhaustedReturnsLastRetryException() throws IOException {
        Integer retryCode = 429;
        int expectedNumberOfRequestAttempts = 2;
        doReturn(retryCode).when(statusLine).getStatusCode();
        doReturn(statusLine).when(response).getStatusLine();
        doReturn(response).when(responseException).getResponse();
        ResponseException initialRetryException = mock(ResponseException.class);
        doReturn(response).when(initialRetryException).getResponse();

        try (RestElasticSearchClient restClientUnderTest = createClient(expectedNumberOfRequestAttempts - 1,
            Sets.newHashSet(retryCode))) {
            //prime the restClientMock again after it's reset after creation
            when(restClientMock.performRequest(any()))
                //first throw a different retry exception instance, then make sure it's the latter one
                //that was retained and then thrown
                .thenThrow(initialRetryException)
                .thenThrow(responseException);


            restClientUnderTest.bulkRequest(Collections.singletonList(
                    ElasticSearchMutation.createDeleteRequest("some_index", "some_type", "some_doc_id")),
                null);
            Assertions.fail("Should have thrown the expected exception after retry");
        } catch (Exception e) {
            Assertions.assertSame(responseException, e);
        }
        verify(restClientMock, times(expectedNumberOfRequestAttempts)).performRequest(requestCaptor.capture());
    }

    @Test
    public void testNonRetryErrorCodeException() throws IOException {
        doReturn(503).when(statusLine).getStatusCode();
        doReturn(statusLine).when(response).getStatusLine();
        doReturn(response).when(responseException).getResponse();
        try (RestElasticSearchClient restClientUnderTest = createClient(0,
            //Other retry error code is configured
            Sets.newHashSet(429))) {
            //prime the restClientMock again after it's reset after creation
            when(restClientMock.performRequest(any()))
                .thenThrow(responseException);
            restClientUnderTest.bulkRequest(Collections.singletonList(
                    ElasticSearchMutation.createDeleteRequest("some_index", "some_type", "some_doc_id")),
                null);
            Assertions.fail("Should have thrown the expected exception");
        } catch (Exception e) {
            Assertions.assertSame(responseException, e);
        }
        verify(restClientMock, times(1)).performRequest(requestCaptor.capture());
    }

    @Test
    public void testNonResponseExceptionErrorThrown() throws IOException {
        IOException differentExceptionType = new IOException();
        when(restClientMock.performRequest(any()))
            .thenThrow(differentExceptionType);
        try (RestElasticSearchClient restClientUnderTest = createClient(0, Collections.emptySet())) {
            restClientUnderTest.bulkRequest(Collections.singletonList(
                    ElasticSearchMutation.createDeleteRequest("some_index", "some_type", "some_doc_id")),
                null);
            Assertions.fail("Should have thrown the expected exception");
        } catch (Exception e) {
            Assertions.assertSame(differentExceptionType, e);
        }
        verify(restClientMock, times(1)).performRequest(requestCaptor.capture());
    }
}
