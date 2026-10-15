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
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.janusgraph.diskstorage.es.ElasticSearchClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A count which only has to reach a limit tells each shard to stop there, and an aggregation asks for the aggregation
 * alone, without hits or a count of the matches.
 */
@ExtendWith(MockitoExtension.class)
public class RestClientCountAndAggregationTest {

    @Mock
    private RestClient restClientMock;

    @Captor
    private ArgumentCaptor<Request> requestCaptor;

    private RestElasticSearchClient createClient(String responseBody) throws IOException {
        when(restClientMock.performRequest(any())).thenThrow(new IOException());
        final RestElasticSearchClient client = new RestElasticSearchClient(restClientMock, 60, false,
            0, Collections.emptySet(), 0, 0, 100_000_000);
        Mockito.reset(restClientMock);
        final StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(200);
        final Response response = mock(Response.class);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(response.getEntity()).thenReturn(new StringEntity(responseBody, ContentType.APPLICATION_JSON));
        when(restClientMock.performRequest(any())).thenReturn(response);
        return client;
    }

    private static Map<String, Object> query() {
        final Map<String, Object> requestData = new HashMap<>();
        requestData.put("query", Collections.singletonMap("match_all", Collections.emptyMap()));
        return requestData;
    }

    private Request requestSent() throws IOException {
        verify(restClientMock).performRequest(requestCaptor.capture());
        return requestCaptor.getValue();
    }

    @Test
    public void shouldStopEachShardOfABoundedCountAtTheBound() throws IOException {
        try (RestElasticSearchClient client = createClient("{\"count\": 7, \"terminated_early\": true}")) {
            assertEquals(7, client.countTotal("store", query(), 25));
            final Request request = requestSent();
            assertEquals("/store/_count", request.getEndpoint());
            assertEquals("25", request.getParameters().get("terminate_after"));
        }
    }

    @Test
    public void shouldCountEverythingWithoutABound() throws IOException {
        try (RestElasticSearchClient client = createClient("{\"count\": 70000}")) {
            assertEquals(70000, client.countTotal("store", query(), 0));
            assertNull(requestSent().getParameters().get("terminate_after"));
        }
    }

    @Test
    public void shouldCountEverythingThroughTheUnboundedMethod() throws IOException {
        try (RestElasticSearchClient client = createClient("{\"count\": 70000}")) {
            assertEquals(70000, client.countTotal("store", query()));
            assertNull(requestSent().getParameters().get("terminate_after"));
        }
    }

    //An implementation which can only count everything satisfies a bounded count as well, which its caller clamps
    @Test
    public void shouldCountEverythingForABoundedCountByDefault() throws IOException {
        final ElasticSearchClient client = mock(ElasticSearchClient.class);
        when(client.countTotal("store", query())).thenReturn(42L);
        when(client.countTotal(eq("store"), any(), anyInt())).thenCallRealMethod();

        assertEquals(42L, client.countTotal("store", query(), 5));
    }

    @Test
    public void shouldAggregateWithoutHitsAndWithoutCountingTheMatches() throws IOException {
        try (RestElasticSearchClient client = createClient("{\"aggregations\": {\"agg_result\": {\"value\": 42.0}}}")) {
            assertEquals(42, client.max("store", query(), "age", Integer.class));
            final Request request = requestSent();
            assertEquals("/store/_search", request.getEndpoint());
            final Map<?, ?> body = new ObjectMapper().readValue(EntityUtils.toString(request.getEntity()), Map.class);
            assertEquals(0, body.get("size"));
            assertEquals(Boolean.FALSE, body.get("track_total_hits"));
            assertEquals(Collections.singletonMap("agg_result", Collections.singletonMap("max",
                Collections.singletonMap("field", "age"))), body.get("aggs"));
        }
    }
}
