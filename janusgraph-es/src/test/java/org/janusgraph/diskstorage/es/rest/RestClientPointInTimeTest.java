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
import org.janusgraph.diskstorage.es.ElasticMajorVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Which clusters page with a point in time, and the requests which open, search and close one. OpenSearch has a point
 * in time API of its own, but neither the {@code _shard_doc} tiebreaker nor sort values for a search sorted by score,
 * so {@code search_after} can't page there, and OpenSearch scrolls.
 */
@ExtendWith(MockitoExtension.class)
public class RestClientPointInTimeTest {

    private static final String INDEX = "janusgraph_vertex";

    @Mock
    private RestClient restClientMock;

    @Captor
    private ArgumentCaptor<Request> requestCaptor;

    private static String rootResponse(String distribution, String number) {
        return "{\"name\":\"node\",\"cluster_name\":\"docker-cluster\",\"version\":{"
            + (distribution != null ? "\"distribution\":\"" + distribution + "\"," : "")
            + "\"number\":\"" + number + "\",\"lucene_version\":\"9.0.0\"},\"tagline\":\"You Know, for Search\"}";
    }

    private static Map<String, Object> version(String distribution, String number) {
        final Map<String, Object> version = new HashMap<>();
        if (distribution != null) {
            version.put("distribution", distribution);
        }
        version.put("number", number);
        return version;
    }

    private static Response response(int statusCode, String body) {
        final StatusLine statusLine = Mockito.mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);
        final Response response = Mockito.mock(Response.class);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(response.getEntity()).thenReturn(new StringEntity(body, ContentType.APPLICATION_JSON));
        return response;
    }

    //A client which asked the cluster for its version and got the given root response
    private RestElasticSearchClient clientOf(String rootResponse) throws IOException {
        final Response root = Mockito.mock(Response.class);
        when(root.getEntity()).thenReturn(new StringEntity(rootResponse, ContentType.APPLICATION_JSON));
        when(restClientMock.performRequest(any())).thenReturn(root);
        final RestElasticSearchClient client = new RestElasticSearchClient(restClientMock, 60, false, 0,
            Collections.emptySet(), 0, 0, 100_000_000);
        Mockito.reset(restClientMock);
        return client;
    }

    private Request onlyRequest() throws IOException {
        verify(restClientMock).performRequest(requestCaptor.capture());
        return requestCaptor.getValue();
    }

    private static Map<?, ?> body(Request request) throws IOException {
        return new ObjectMapper().readValue(EntityUtils.toString(request.getEntity()), Map.class);
    }

    @Test
    public void shouldKnowWhichVersionsPageWithAPointInTime() {
        assertFalse(RestElasticSearchClient.clusterSupportsPointInTime(version(null, "7.11.2")));
        assertTrue(RestElasticSearchClient.clusterSupportsPointInTime(version(null, "7.12.0")));
        assertTrue(RestElasticSearchClient.clusterSupportsPointInTime(version(null, "7.17.8")));
        assertTrue(RestElasticSearchClient.clusterSupportsPointInTime(version(null, "8.0.0")));
        assertTrue(RestElasticSearchClient.clusterSupportsPointInTime(version(null, "9.5.4")));
        assertFalse(RestElasticSearchClient.clusterSupportsPointInTime(version("opensearch", "2.19.6")));
        assertFalse(RestElasticSearchClient.clusterSupportsPointInTime(version("opensearch", "3.9.0")));
        assertFalse(RestElasticSearchClient.clusterSupportsPointInTime(version(null, "7")));
        assertFalse(RestElasticSearchClient.clusterSupportsPointInTime(null));
    }

    @Test
    public void shouldPageWithAPointInTimeWhereTheClusterReportsOne() throws IOException {
        try (RestElasticSearchClient client = clientOf(rootResponse(null, "7.17.8"))) {
            assertTrue(client.supportsPointInTime());
        }
        try (RestElasticSearchClient client = clientOf(rootResponse(null, "7.11.2"))) {
            assertFalse(client.supportsPointInTime());
        }
        try (RestElasticSearchClient client = clientOf(rootResponse("opensearch", "3.9.0"))) {
            assertFalse(client.supportsPointInTime());
        }
    }

    //A cluster whose version can't be asked for isn't known to have the API
    @Test
    public void shouldScrollWhereTheClustersVersionCannotBeAsked() throws IOException {
        when(restClientMock.performRequest(any())).thenThrow(new IOException("connection refused"));
        try (RestElasticSearchClient client = new RestElasticSearchClient(restClientMock, 60, false, 0,
            Collections.emptySet(), 0, 0, 100_000_000)) {
            assertEquals(ElasticMajorVersion.NINE, client.getMajorVersion());
            assertFalse(client.supportsPointInTime());
        }
    }

    //Without asking the cluster, only a major version whose every release has the API is known to have it
    @Test
    public void shouldPageWithAPointInTimeForAConfiguredMajorVersionOfEightOrLater() throws IOException {
        try (RestElasticSearchClient client = new RestElasticSearchClient(restClientMock, 60, false, 0,
            Collections.emptySet(), 0, 0, 100_000_000, ElasticMajorVersion.SEVEN)) {
            assertFalse(client.supportsPointInTime());
        }
        try (RestElasticSearchClient client = new RestElasticSearchClient(restClientMock, 60, false, 0,
            Collections.emptySet(), 0, 0, 100_000_000, ElasticMajorVersion.EIGHT)) {
            assertTrue(client.supportsPointInTime());
            client.setPointInTimeEnabled(false);
            assertFalse(client.supportsPointInTime());
        }
        verify(restClientMock, never()).performRequest(any());
    }

    @Test
    public void shouldOpenAPointInTime() throws IOException {
        try (RestElasticSearchClient client = clientOf(rootResponse(null, "8.19.0"))) {
            final Response stubbed = response(200, "{\"id\":\"pit-1\"}");
            when(restClientMock.performRequest(any())).thenReturn(stubbed);
            assertEquals("pit-1", client.openPointInTime(INDEX));
            final Request request = onlyRequest();
            assertEquals("POST", request.getMethod());
            assertEquals("/" + INDEX + "/_pit", request.getEndpoint());
            assertEquals("60s", request.getParameters().get("keep_alive"));
            assertNull(request.getEntity());
        }
    }

    @Test
    public void shouldSearchAPointInTimeWithoutAnIndexInThePath() throws IOException {
        try (RestElasticSearchClient client = clientOf(rootResponse(null, "8.19.0"))) {
            final Response stubbed = response(200, "{\"took\":3,\"pit_id\":\"pit-2\","
                + "\"hits\":{\"hits\":[{\"_id\":\"a\",\"_score\":1.5,\"sort\":[1.5,7]},"
                + "{\"_id\":\"b\",\"_score\":1.0,\"sort\":[1.0,12]}]}}");
            when(restClientMock.performRequest(any())).thenReturn(stubbed);
            final Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("query", Collections.singletonMap("match_all", Collections.emptyMap()));
            requestBody.put("search_after", Arrays.asList(2.0, 3));
            final RestSearchResponse searchResponse = client.searchPointInTime("pit-1", requestBody);
            final Request request = onlyRequest();
            assertEquals("/_search", request.getEndpoint());
            final Map<?, ?> body = body(request);
            assertEquals(Collections.singletonMap("id", "pit-1"), withoutKeepAlive((Map<?, ?>) body.get("pit")));
            assertEquals("60s", ((Map<?, ?>) body.get("pit")).get("keep_alive"));
            assertEquals(Arrays.asList(2.0, 3), body.get("search_after"));
            assertEquals("pit-2", searchResponse.getPitId());
            assertEquals(2, searchResponse.numResults());
            assertEquals(Arrays.asList(1.0, 12), searchResponse.getLastSort());
        }
    }

    private static Map<?, ?> withoutKeepAlive(Map<?, ?> pit) {
        final Map<Object, Object> copy = new HashMap<>(pit);
        copy.remove("keep_alive");
        return copy;
    }

    @Test
    public void shouldCloseAPointInTimeAsynchronously() throws IOException {
        try (RestElasticSearchClient client = clientOf(rootResponse(null, "8.19.0"))) {
            client.closePointInTime("pit-1");
            verify(restClientMock).performRequestAsync(requestCaptor.capture(), any());
            verify(restClientMock, never()).performRequest(any());
            final Request request = requestCaptor.getValue();
            assertEquals("DELETE", request.getMethod());
            assertEquals("/_pit", request.getEndpoint());
            assertEquals(Collections.singletonMap("id", "pit-1"), body(request));
        }
    }

}
