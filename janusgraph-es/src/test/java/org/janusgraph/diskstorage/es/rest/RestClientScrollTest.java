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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.StatusLine;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Releasing a scroll context is a courtesy to the cluster which must cost the search nothing: it goes out
 * asynchronously, in the body form which every supported version accepts, and its failure is not the caller's.
 */
@ExtendWith(MockitoExtension.class)
public class RestClientScrollTest {

    @Mock
    private RestClient restClientMock;

    @Captor
    private ArgumentCaptor<Request> requestCaptor;

    @Captor
    private ArgumentCaptor<ResponseListener> listenerCaptor;

    private RestElasticSearchClient createClient() throws IOException {
        when(restClientMock.performRequest(any())).thenThrow(new IOException());
        final RestElasticSearchClient client = new RestElasticSearchClient(restClientMock, 60, false,
            0, Collections.emptySet(), 0, 0, 100_000_000);
        Mockito.reset(restClientMock);
        return client;
    }

    @Test
    public void shouldReleaseTheScrollAsynchronouslyWithTheIdInTheBody() throws IOException {
        try (RestElasticSearchClient client = createClient()) {
            client.deleteScroll("DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAD4WYm9laVYtZndUQlNsdDcwakFMNjU1QQ==");

            verify(restClientMock).performRequestAsync(requestCaptor.capture(), listenerCaptor.capture());
            verify(restClientMock, never()).performRequest(any());
            final Request request = requestCaptor.getValue();
            assertEquals("DELETE", request.getMethod());
            assertEquals("/_search/scroll", request.getEndpoint());
            final Map<?, ?> body = new ObjectMapper().readValue(EntityUtils.toString(request.getEntity()), Map.class);
            assertEquals(Collections.singletonMap("scroll_id",
                Collections.singletonList("DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAD4WYm9laVYtZndUQlNsdDcwakFMNjU1QQ==")), body);
        }
    }

    @Test
    public void shouldNotFailWhenTheReleaseFails() throws IOException {
        try (RestElasticSearchClient client = createClient()) {
            client.deleteScroll("scroll-id");

            verify(restClientMock).performRequestAsync(any(), listenerCaptor.capture());
            //The listener runs on the client's I/O thread, so whatever it does with the failure must stay there
            listenerCaptor.getValue().onFailure(new IOException("connection reset"));
        }
    }

    //Only a rejection which every later release will meet as well is worth the one warning: a request the cluster
    //doesn't accept or permit. A context which is gone already, a busy cluster, a timed out request and a server
    //error are passing, and must not use the warning up
    @Test
    public void shouldWarnOnlyForARejectionWhichWillRepeat() {
        assertTrue(RestElasticSearchClient.isRejectedScrollRelease(400));
        assertTrue(RestElasticSearchClient.isRejectedScrollRelease(401));
        assertTrue(RestElasticSearchClient.isRejectedScrollRelease(403));
        assertTrue(RestElasticSearchClient.isRejectedScrollRelease(405));
        assertFalse(RestElasticSearchClient.isRejectedScrollRelease(404));
        assertFalse(RestElasticSearchClient.isRejectedScrollRelease(408));
        assertFalse(RestElasticSearchClient.isRejectedScrollRelease(429));
        assertFalse(RestElasticSearchClient.isRejectedScrollRelease(500));
        assertFalse(RestElasticSearchClient.isRejectedScrollRelease(502));
        assertFalse(RestElasticSearchClient.isRejectedScrollRelease(503));
        assertFalse(RestElasticSearchClient.isRejectedScrollRelease(504));
    }

    //A rejected release (for example for want of the privilege to clear scrolls) is worth a warning, and still not
    //the search's failure
    @Test
    public void shouldNotFailWhenTheReleaseIsRejected() throws IOException {
        final StatusLine forbidden = Mockito.mock(StatusLine.class);
        when(forbidden.getStatusCode()).thenReturn(403);
        final Response response = Mockito.mock(Response.class);
        when(response.getStatusLine()).thenReturn(forbidden);
        final ResponseException rejection = Mockito.mock(ResponseException.class);
        when(rejection.getResponse()).thenReturn(response);
        try (RestElasticSearchClient client = createClient()) {
            client.deleteScroll("scroll-1");
            client.deleteScroll("scroll-2");

            verify(restClientMock, Mockito.times(2)).performRequestAsync(any(), listenerCaptor.capture());
            for (ResponseListener listener : listenerCaptor.getAllValues()) {
                listener.onFailure(rejection);
            }
        }
    }
}
