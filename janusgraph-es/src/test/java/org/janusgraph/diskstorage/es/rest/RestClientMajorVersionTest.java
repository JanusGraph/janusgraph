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
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The client asks the cluster for its version unless the major version of the Elasticsearch API is configured.
 */
@ExtendWith(MockitoExtension.class)
public class RestClientMajorVersionTest {

    //Nothing here submits a bulk request, so the value only has to be the default rather than anything meaningful
    private static final int BULK_CHUNK_SIZE_LIMIT_BYTES = 100_000_000;

    //The root endpoint of OpenSearch 3.9.0
    private static final String OPENSEARCH_ROOT_RESPONSE = "{\"name\":\"node\",\"cluster_name\":\"docker-cluster\"," +
        "\"version\":{\"distribution\":\"opensearch\",\"number\":\"3.9.0\",\"build_type\":\"tar\"," +
        "\"lucene_version\":\"10.3.2\",\"minimum_wire_compatibility_version\":\"2.19.0\"," +
        "\"minimum_index_compatibility_version\":\"2.0.0\"},\"tagline\":\"The OpenSearch Project: https://opensearch.org/\"}";

    //The root endpoint of Elasticsearch 7.17.8
    private static final String ELASTICSEARCH_7_ROOT_RESPONSE = "{\"name\":\"node\",\"cluster_name\":\"docker-cluster\"," +
        "\"version\":{\"number\":\"7.17.8\",\"build_flavor\":\"default\",\"lucene_version\":\"8.11.1\"," +
        "\"minimum_wire_compatibility_version\":\"6.8.0\",\"minimum_index_compatibility_version\":\"6.0.0-beta1\"}," +
        "\"tagline\":\"You Know, for Search\"}";

    @Mock
    private RestClient restClientMock;

    @Captor
    private ArgumentCaptor<Request> requestCaptor;

    private RestElasticSearchClient createClient(ElasticMajorVersion majorVersion) {
        return createClient(majorVersion, false);
    }

    private RestElasticSearchClient createClient(ElasticMajorVersion majorVersion, boolean useMappingTypesForES7) {
        return new RestElasticSearchClient(restClientMock, 0, useMappingTypesForES7, 0, Collections.emptySet(), 0, 0,
            BULK_CHUNK_SIZE_LIMIT_BYTES, majorVersion);
    }

    private Response rootResponse(String body) {
        final Response response = Mockito.mock(Response.class);
        when(response.getEntity()).thenReturn(new StringEntity(body, ContentType.APPLICATION_JSON));
        return response;
    }

    private Request createIndexRequest(String rootResponse) throws IOException {
        final Response response = rootResponse(rootResponse);
        when(restClientMock.performRequest(any())).thenReturn(response);
        try (RestElasticSearchClient clientUnderTest = createClient(null, true)) {
            Mockito.reset(restClientMock);
            final StatusLine created = Mockito.mock(StatusLine.class);
            when(created.getStatusCode()).thenReturn(200);
            final Response createResponse = Mockito.mock(Response.class);
            when(createResponse.getStatusLine()).thenReturn(created);
            when(restClientMock.performRequest(any())).thenReturn(createResponse);
            clientUnderTest.createIndex("janusgraph_vertex", null);
            verify(restClientMock).performRequest(requestCaptor.capture());
            return requestCaptor.getValue();
        }
    }

    @Test
    public void testConfiguredMajorVersionIsUsedWithoutAskingTheCluster() throws IOException {
        try (RestElasticSearchClient clientUnderTest = createClient(ElasticMajorVersion.SEVEN)) {
            assertEquals(ElasticMajorVersion.SEVEN, clientUnderTest.getMajorVersion());
        }
        verify(restClientMock, never()).performRequest(any());
    }

    @Test
    public void testOpenSearchIsDetected() throws IOException {
        final Response response = rootResponse(OPENSEARCH_ROOT_RESPONSE);
        when(restClientMock.performRequest(any())).thenReturn(response);
        try (RestElasticSearchClient clientUnderTest = createClient(null)) {
            assertEquals(ElasticMajorVersion.SEVEN, clientUnderTest.getMajorVersion());
        }
    }

    @Test
    public void testElasticsearch7UsesMappingTypesWhenAskedTo() throws IOException {
        final Request request = createIndexRequest(ELASTICSEARCH_7_ROOT_RESPONSE);
        assertEquals("true", request.getParameters().get(RestElasticSearchClient.INCLUDE_TYPE_NAME_PARAMETER));
    }

    @Test
    public void testOpenSearchDoesNotUseMappingTypes() throws IOException {
        final Request request = createIndexRequest(OPENSEARCH_ROOT_RESPONSE);
        assertFalse(request.getParameters().containsKey(RestElasticSearchClient.INCLUDE_TYPE_NAME_PARAMETER));
    }
}
