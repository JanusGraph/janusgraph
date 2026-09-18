// Copyright 2026 JanusGraph Authors
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

import org.apache.http.StatusLine;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

//clearStore deletes the Elasticsearch index which backs a store, and is reached through
//SchemaAction.DISCARD_INDEX. It must use the index name it is given verbatim, because the caller has already mapped
//the JanusGraph store name to it. Deriving the name a second time here is what let a store name containing an
//uppercase character target an index which cannot exist, since Elasticsearch index names are always lowercase.
@ExtendWith(MockitoExtension.class)
public class RestClientClearStoreTest {

    private static final String INDEX_STORE_NAME = "janusgraph_vertexbyname";

    @Mock
    private RestClient restClientMock;

    @Mock
    private StatusLine statusLine;

    @Captor
    private ArgumentCaptor<Request> requestCaptor;

    private RestElasticSearchClient createClient() throws IOException {
        //The version lookup during instantiation is not under test
        when(restClientMock.performRequest(any())).thenThrow(new IOException());
        final RestElasticSearchClient clientUnderTest = new RestElasticSearchClient(restClientMock, 0, false,
            0, Collections.emptySet(), 0, 0, 100_000_000);
        Mockito.reset(restClientMock);
        return clientUnderTest;
    }

    private Response response(int statusCode) {
        when(statusLine.getStatusCode()).thenReturn(statusCode);
        final Response response = Mockito.mock(Response.class);
        when(response.getStatusLine()).thenReturn(statusLine);
        return response;
    }

    @Test
    public void shouldDeleteTheIndexItIsGiven() throws IOException {
        try (RestElasticSearchClient clientUnderTest = createClient()) {
            final Response found = response(200);
            when(restClientMock.performRequest(any())).thenReturn(found);
            clientUnderTest.clearStore(INDEX_STORE_NAME);

            //The existence check, then the deletion, both against the given name
            verify(restClientMock, Mockito.times(2)).performRequest(requestCaptor.capture());
            final List<Request> requests = requestCaptor.getAllValues();
            assertEquals("HEAD", requests.get(0).getMethod());
            assertEquals("/" + INDEX_STORE_NAME, requests.get(0).getEndpoint());
            assertEquals("DELETE", requests.get(1).getMethod());
            assertEquals("/" + INDEX_STORE_NAME, requests.get(1).getEndpoint());
        }
    }

    @Test
    public void shouldNotDeleteAnIndexWhichDoesNotExist() throws IOException {
        try (RestElasticSearchClient clientUnderTest = createClient()) {
            final Response notFound = response(404);
            when(restClientMock.performRequest(any())).thenReturn(notFound);
            clientUnderTest.clearStore(INDEX_STORE_NAME);

            //Only the existence check is issued, so no deletion followed it
            verify(restClientMock, Mockito.times(1)).performRequest(requestCaptor.capture());
            assertEquals("HEAD", requestCaptor.getValue().getMethod());
        }
    }

    @Test
    public void shouldNotLowercaseTheNameItIsGiven() throws IOException {
        //Deriving the name is the caller's job. If this method lowercased as well, a store whose Elasticsearch index
        //genuinely differs in case could not be addressed at all
        final String mixedCase = "Janusgraph_MixedCase";
        try (RestElasticSearchClient clientUnderTest = createClient()) {
            final Response found = response(200);
            when(restClientMock.performRequest(any())).thenReturn(found);
            clientUnderTest.clearStore(mixedCase);

            verify(restClientMock, Mockito.times(2)).performRequest(requestCaptor.capture());
            assertTrue(requestCaptor.getAllValues().stream()
                .allMatch(request -> request.getEndpoint().equals("/" + mixedCase)));
        }
    }
}
