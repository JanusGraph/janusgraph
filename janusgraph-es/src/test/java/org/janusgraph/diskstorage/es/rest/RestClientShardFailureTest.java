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
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.janusgraph.diskstorage.es.rest.RestElasticSearchClient.ALLOW_PARTIAL_SEARCH_RESULTS_PARAMETER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Elasticsearch answers a search, a page of a scroll or of a point in time, a count or an aggregation which some shards
 * failed with the results of the others, unless every shard failed or it was told not to allow partial results. So the
 * client tells it so wherever a request takes the parameter, and fails every answer which lacks the results of some
 * shards. The failures below are those Elasticsearch 9.5.4 reported on an index of two shards, one of which failed,
 * shortened; OpenSearch 3.9.0 reported the same, but for a shard which wasn't available.
 */
@ExtendWith(MockitoExtension.class)
public class RestClientShardFailureTest {

    private static final String INDEX = "janusgraph_vertex";

    //A script field which threw for a document of shard 0
    private static final String SCRIPT_FAILURE = "{\"shard\":0,\"index\":\"" + INDEX + "\","
        + "\"node\":\"p_Fuu6-xTNWsZMhLOUsgIA\",\"reason\":{\"type\":\"script_exception\",\"reason\":\"runtime error\","
        + "\"lang\":\"painless\",\"caused_by\":{\"type\":\"illegal_argument_exception\",\"reason\":\"boom\"}}}";

    //A shard which no longer had the context of a scroll, which a page of a scroll can't name
    private static final String LOST_CONTEXT = "{\"shard\":-1,\"index\":null,\"reason\":{\"type\":"
        + "\"search_context_missing_exception\",\"reason\":\"No search context found for id [26]\"}}";

    //A shard which wasn't available, which Elasticsearch 7.17.8 and 9.5.4 count as failed. OpenSearch 2.19.6 and 3.9.0
    //count it as neither failed nor successful, and list no failure for it
    private static final String UNAVAILABLE = "{\"shard\":1,\"index\":\"" + INDEX + "\",\"node\":null,"
        + "\"reason\":{\"type\":\"no_shard_available_action_exception\",\"reason\":null}}";

    private static final String ALLOW_THEM =
        " Set index.[X].elasticsearch.allow-partial-results to true to use such results.";

    @Mock
    private RestClient restClientMock;

    @Captor
    private ArgumentCaptor<Request> requestCaptor;

    private static String shards(int total, int successful, int failed, String... failures) {
        return "\"_shards\":{\"total\":" + total + ",\"successful\":" + successful + ",\"skipped\":0,"
            + "\"failed\":" + failed
            + (failures.length == 0 ? "" : ",\"failures\":[" + String.join(",", failures) + "]") + "}";
    }

    //An answer which reads as a search, a page of a scroll or of a point in time, a count and an aggregation alike
    private static String answer(String shards) {
        return answer(shards, false);
    }

    private static String answer(String shards, boolean timedOut) {
        return "{\"took\":2,\"timed_out\":" + timedOut + ",\"_scroll_id\":\"scroll-1\",\"pit_id\":\"pit-1\","
            + (shards.isEmpty() ? "" : shards + ",")
            + "\"hits\":{\"total\":{\"value\":7,\"relation\":\"eq\"},"
            + "\"hits\":[{\"_id\":\"d1\",\"_score\":1.0}]},"
            + "\"aggregations\":{\"agg_result\":{\"value\":9.0}}}";
    }

    //A client of the given major version which gets the given answer to every request
    private RestElasticSearchClient client(ElasticMajorVersion version, String answer) throws IOException {
        final StatusLine ok = mock(StatusLine.class);
        when(ok.getStatusCode()).thenReturn(200);
        final Response response = mock(Response.class);
        when(response.getStatusLine()).thenReturn(ok);
        when(response.getEntity()).thenReturn(new StringEntity(answer, ContentType.APPLICATION_JSON));
        when(restClientMock.performRequest(any())).thenReturn(response);
        return new RestElasticSearchClient(restClientMock, 60, false, 0, Collections.emptySet(), 0, 0,
            100_000_000, version);
    }

    private static Map<String, Object> query() {
        final Map<String, Object> requestData = new HashMap<>();
        requestData.put("query", Collections.singletonMap("match_all", Collections.emptyMap()));
        return requestData;
    }

    @Test
    public void shouldFailASearchWhichSomeShardsFailed() throws IOException {
        try (RestElasticSearchClient client = client(ElasticMajorVersion.NINE,
                answer(shards(2, 1, 1, SCRIPT_FAILURE)))) {
            assertEquals("Elasticsearch answered a search with the results of only 1 of 2 shards: [" + INDEX + "][0] "
                    + "script_exception: runtime error (caused by illegal_argument_exception: boom)." + ALLOW_THEM,
                assertThrows(IOException.class, () -> client.search(INDEX, query(), false)).getMessage());
        }
    }

    @Test
    public void shouldFailAPageOfAScrollWhichSomeShardsFailed() throws IOException {
        try (RestElasticSearchClient client = client(ElasticMajorVersion.NINE,
                answer(shards(2, 1, 1, LOST_CONTEXT)))) {
            assertEquals("Elasticsearch answered a page of a scroll with the results of only 1 of 2 shards: "
                    + "search_context_missing_exception: No search context found for id [26]." + ALLOW_THEM,
                assertThrows(IOException.class, () -> client.search("scroll-1")).getMessage());
        }
    }

    @Test
    public void shouldFailAPageOfAPointInTimeWhichSomeShardsFailed() throws IOException {
        try (RestElasticSearchClient client = client(ElasticMajorVersion.NINE,
                answer(shards(2, 1, 1, UNAVAILABLE)))) {
            assertEquals("Elasticsearch answered a page of a point in time with the results of only 1 of 2 shards: ["
                    + INDEX + "][1] no_shard_available_action_exception." + ALLOW_THEM,
                assertThrows(IOException.class, () -> client.searchPointInTime("pit-1", query())).getMessage());
        }
    }

    //As OpenSearch answers a search on an index one of whose shards isn't available
    @Test
    public void shouldFailASearchWhichAShardWasNotAvailableFor() throws IOException {
        try (RestElasticSearchClient client = client(ElasticMajorVersion.SEVEN, answer(shards(2, 1, 0)))) {
            assertEquals("Elasticsearch answered a search with the results of only 1 of 2 shards." + ALLOW_THEM,
                assertThrows(IOException.class, () -> client.search(INDEX, query(), true)).getMessage());
        }
    }

    @Test
    public void shouldFailACountWhichSomeShardsFailed() throws IOException {
        try (RestElasticSearchClient client = client(ElasticMajorVersion.NINE,
                answer(shards(2, 1, 1, SCRIPT_FAILURE)))) {
            assertEquals("Elasticsearch answered a count with the results of only 1 of 2 shards: [" + INDEX + "][0] "
                    + "script_exception: runtime error (caused by illegal_argument_exception: boom)." + ALLOW_THEM,
                assertThrows(IOException.class, () -> client.countTotal(INDEX, query(), 5)).getMessage());
        }
    }

    @Test
    public void shouldFailAnAggregationWhichSomeShardsFailed() throws IOException {
        try (RestElasticSearchClient client = client(ElasticMajorVersion.NINE,
                answer(shards(2, 1, 1, SCRIPT_FAILURE, UNAVAILABLE)))) {
            assertEquals("Elasticsearch answered an aggregation with the results of only 1 of 2 shards: [" + INDEX
                    + "][0] script_exception: runtime error (caused by illegal_argument_exception: boom); [" + INDEX
                    + "][1] no_shard_available_action_exception." + ALLOW_THEM,
                assertThrows(IOException.class, () -> client.max(INDEX, query(), "time", Long.class)).getMessage());
        }
    }

    //Where Elasticsearch isn't told not to allow partial results, as on Elasticsearch 6, a shard which runs out of the
    //time of a search answers with the hits it found until then, and counts as successful
    @Test
    public void shouldFailASearchWhoseShardsRanOutOfTime() throws IOException {
        try (RestElasticSearchClient client = client(ElasticMajorVersion.SIX, answer(shards(2, 2, 0), true))) {
            assertEquals("Elasticsearch answered a search with the results which its shards found before they ran out "
                    + "of time." + ALLOW_THEM,
                assertThrows(IOException.class, () -> client.search(INDEX, query(), false)).getMessage());
        }
    }

    @Test
    public void shouldFailACountWhoseShardsRanOutOfTime() throws IOException {
        try (RestElasticSearchClient client = client(ElasticMajorVersion.SIX, answer(shards(2, 2, 0), true))) {
            assertEquals("Elasticsearch answered a count with the results which its shards found before they ran out "
                    + "of time." + ALLOW_THEM,
                assertThrows(IOException.class, () -> client.countTotal(INDEX, query())).getMessage());
        }
    }

    @Test
    public void shouldFailAnAggregationWhoseShardsRanOutOfTime() throws IOException {
        try (RestElasticSearchClient client = client(ElasticMajorVersion.SIX, answer(shards(2, 2, 0), true))) {
            assertEquals("Elasticsearch answered an aggregation with the results which its shards found before they "
                    + "ran out of time." + ALLOW_THEM,
                assertThrows(IOException.class, () -> client.max(INDEX, query(), "time", Long.class)).getMessage());
        }
    }

    @Test
    public void shouldUseTheResultsFoundInTimeWhereAllowed() throws IOException {
        try (RestElasticSearchClient client = client(ElasticMajorVersion.SIX, answer(shards(2, 2, 0), true))) {
            client.setAllowPartialResults(true);
            assertReadEveryWay(client);
        }
    }

    //Shards which can't match are skipped, and counted as successful
    @Test
    public void shouldUseTheAnswersOfEveryShard() throws IOException {
        try (RestElasticSearchClient client = client(ElasticMajorVersion.NINE,
                answer("\"_shards\":{\"total\":3,\"successful\":3,\"skipped\":2,\"failed\":0}"))) {
            assertReadEveryWay(client);
        }
    }

    @Test
    public void shouldUseAnAnswerWhichDoesNotTellItsShards() throws IOException {
        try (RestElasticSearchClient client = client(ElasticMajorVersion.NINE, answer(""))) {
            assertReadEveryWay(client);
        }
    }

    @Test
    public void shouldUseTheResultsOfTheShardsWhichAnsweredWhereAllowed() throws IOException {
        try (RestElasticSearchClient client = client(ElasticMajorVersion.NINE,
                answer(shards(2, 1, 1, SCRIPT_FAILURE)))) {
            client.setAllowPartialResults(true);
            assertReadEveryWay(client);
        }
    }

    //Every request to the search API, a count among them, tells Elasticsearch 7 and later, which OpenSearch reports to
    //be, not to allow partial results. A later page of a scroll doesn't take the parameter: Elasticsearch rejects it
    @ParameterizedTest
    @EnumSource(value = ElasticMajorVersion.class, names = {"SEVEN", "EIGHT", "NINE"})
    public void shouldTellElasticsearchNotToAllowPartialResultsWhereARequestTakesIt(ElasticMajorVersion version)
            throws IOException {
        try (RestElasticSearchClient client = client(version, answer(shards(2, 2, 0)))) {
            final List<Request> requests = readEveryWay(client);
            assertEquals(Arrays.asList("false", "false", null, "false", "false", "false"),
                allowPartialResults(requests));
            assertEquals("/" + INDEX + "/_search", requests.get(1).getEndpoint());
            assertEquals("60s", requests.get(1).getParameters().get("scroll"));
            assertEquals("/" + INDEX + "/_search", requests.get(4).getEndpoint());
        }
    }

    @Test
    public void shouldNotTellElasticsearch6WhichOnlyGetsItsAnswersChecked() throws IOException {
        try (RestElasticSearchClient client = client(ElasticMajorVersion.SIX, answer(shards(2, 2, 0)))) {
            assertEquals(Collections.nCopies(6, null), allowPartialResults(readEveryWay(client)));
        }
    }

    @Test
    public void shouldLeaveElasticsearchItsDefaultWherePartialResultsAreAllowed() throws IOException {
        try (RestElasticSearchClient client = client(ElasticMajorVersion.NINE, answer(shards(2, 2, 0)))) {
            client.setAllowPartialResults(true);
            assertEquals(Collections.nCopies(6, null), allowPartialResults(readEveryWay(client)));
        }
    }

    //A search, the first page of a scroll, a later page, a page of a point in time, a count and an aggregation
    private List<Request> readEveryWay(RestElasticSearchClient client) throws IOException {
        client.search(INDEX, query(), false);
        client.search(INDEX, query(), true);
        client.search("scroll-1");
        client.searchPointInTime("pit-1", query());
        client.countTotal(INDEX, query());
        client.max(INDEX, query(), "time", Long.class);
        verify(restClientMock, times(6)).performRequest(requestCaptor.capture());
        return requestCaptor.getAllValues();
    }

    private void assertReadEveryWay(RestElasticSearchClient client) throws IOException {
        assertEquals(Collections.singletonList("d1"), ids(client.search(INDEX, query(), false)));
        assertEquals(Collections.singletonList("d1"), ids(client.search("scroll-1")));
        assertEquals(Collections.singletonList("d1"), ids(client.searchPointInTime("pit-1", query())));
        assertEquals(7, client.countTotal(INDEX, query()));
        assertEquals(9L, client.max(INDEX, query(), "time", Long.class));
    }

    private static List<String> ids(RestSearchResponse response) {
        return response.getResults().map(RawQuery.Result::getResult).collect(Collectors.toList());
    }

    private static List<String> allowPartialResults(List<Request> requests) {
        return requests.stream().map(request -> request.getParameters().get(ALLOW_PARTIAL_SEARCH_RESULTS_PARAMETER))
            .collect(Collectors.toList());
    }
}
