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

package org.janusgraph.diskstorage.es;

import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.es.compat.ES7Compat;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.graphdb.query.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * How a search is fetched: in one request when a limit bounds it, otherwise with a first request that finds out
 * whether the result fits into a page, and a scroll only when it does not.
 */
public class ElasticSearchSearcherTest {

    private static final String INDEX = "janusgraph_vertex";
    private static final int PAGE_SIZE = 3;
    private static final Parameter[] NO_PARAMETERS = null;

    private ElasticSearchClient client;
    private ElasticSearchSearcher searcher;

    @BeforeEach
    public void setUp() {
        client = mock(ElasticSearchClient.class);
        //Pages through a point in time wherever the cluster has one; the tests of the other modes make a searcher of
        //their own
        searcher = searcher(PAGE_SIZE, ElasticSearchPagingMode.POINT_IN_TIME);
    }

    //The bounds of the adaptive mode by default: one shard and pages of up to 500
    private ElasticSearchSearcher searcher(int pageSize, ElasticSearchPagingMode pagingMode) {
        return new ElasticSearchSearcher(client, new ES7Compat(), pageSize, pagingMode, 1, 500);
    }

    @Test
    public void shouldFetchALimitedResultWithOneRequestOfTheLimitsSize() throws IOException {
        final List<RawQuery.Result<String>> hits = hits("a", "b", "c", "d", "e");
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(hits, null));

        final List<String> found = ids(searcher.search(INDEX, request(), NO_PARAMETERS, 0, 5));

        assertEquals(ids(hits.stream()), found);
        final Map<String, Object> body = onlySearchBody(false);
        assertEquals(5, body.get("size"));
        assertEquals(0, body.get("from"));
        assertEquals(false, body.get("track_total_hits"));
        verifyNoScroll();
    }

    @Test
    public void shouldFetchALimitSmallerThanAPageWithOneRequest() throws IOException {
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(hits("a", "b"), null));

        assertEquals(2, searcher.search(INDEX, request(), NO_PARAMETERS, 0, 2).count());

        assertEquals(2, onlySearchBody(false).get("size"));
        verifyNoScroll();
    }

    @Test
    public void shouldFetchALimitOfManyPagesWithOneRequest() throws IOException {
        final List<RawQuery.Result<String>> hits = hits(IntStream.range(0, 100).mapToObj(Integer::toString).toArray(String[]::new));
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(hits, null));

        assertEquals(ids(hits.stream()), ids(searcher.search(INDEX, request(), NO_PARAMETERS, 0, 100)));

        assertEquals(100, onlySearchBody(false).get("size"));
        verifyNoScroll();
    }

    //The offset of a raw query is applied by Elasticsearch, so the hits come back already skipped
    @Test
    public void shouldPassTheOffsetToElasticsearchWhenTheLimitBoundsTheResult() throws IOException {
        final List<RawQuery.Result<String>> hits = hits("c", "d", "e");
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(hits, null));

        assertEquals(ids(hits.stream()), ids(searcher.search(INDEX, request(), NO_PARAMETERS, 2, 3)));

        final Map<String, Object> body = onlySearchBody(false);
        assertEquals(2, body.get("from"));
        assertEquals(3, body.get("size"));
        verifyNoScroll();
    }

    @Test
    public void shouldKeepTheCallersParametersInTheRequest() throws IOException {
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(hits("a"), null));

        searcher.search(INDEX, request(), new Parameter[]{new Parameter<>("min_score", 0.5)}, 0, 1);

        assertEquals(0.5, onlySearchBody(false).get("min_score"));
    }

    @Test
    public void shouldNotRequestAnythingForALimitOfZero() throws IOException {
        assertEquals(0, searcher.search(INDEX, request(), NO_PARAMETERS, 0, 0).count());

        verify(client, never()).search(anyString(), any(), eq(false));
        verifyNoScroll();
    }

    //Without a limit the first request asks for one hit more than a page: when fewer come back, that is the
    //whole result and no scroll context is ever opened
    @Test
    public void shouldFetchAnUnlimitedResultWhichFitsIntoAPageWithOneRequest() throws IOException {
        final List<RawQuery.Result<String>> hits = hits("a", "b");
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(hits, null));

        assertEquals(ids(hits.stream()), ids(searcher.search(INDEX, request(), NO_PARAMETERS, 0, Query.NO_LIMIT)));

        final Map<String, Object> body = onlySearchBody(false);
        assertEquals(PAGE_SIZE + 1, body.get("size"));
        assertEquals(0, body.get("from"));
        assertEquals(false, body.get("track_total_hits"));
        verifyNoScroll();
    }

    @Test
    public void shouldFetchAnUnlimitedResultOfExactlyAPageWithOneRequest() throws IOException {
        final List<RawQuery.Result<String>> hits = hits("a", "b", "c");
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(hits, null));

        assertEquals(ids(hits.stream()), ids(searcher.search(INDEX, request(), NO_PARAMETERS, 0, Query.NO_LIMIT)));

        verifyNoScroll();
    }

    //The first request starts at the offset, so a result which fits into a page after the offset needs no scroll
    //however large the offset
    @Test
    public void shouldAskElasticsearchToApplyTheOffsetOfAnUnlimitedResult() throws IOException {
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(hits("c"), null));

        assertEquals(Collections.singletonList("c"), ids(searcher.search(INDEX, request(), NO_PARAMETERS, 2, Query.NO_LIMIT)));

        final Map<String, Object> body = onlySearchBody(false);
        assertEquals(2, body.get("from"));
        assertEquals(PAGE_SIZE + 1, body.get("size"));
        verifyNoScroll();
    }

    @Test
    public void shouldNotRequestAnythingButTheFirstRequestForAnOffsetBeyondTheResult() throws IOException {
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(hits(), null));

        assertEquals(0, searcher.search(INDEX, request(), NO_PARAMETERS, 50, Query.NO_LIMIT).count());

        assertEquals(50, onlySearchBody(false).get("from"));
        verifyNoScroll();
    }

    //When the first request comes back full, the result is larger than a page and a scroll takes over from the
    //start, so the hits are the scroll's and none of the first request's are counted twice
    @Test
    public void shouldScrollAnUnlimitedResultLargerThanAPage() throws IOException {
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(hits("a", "b", "c", "d"), null));
        when(client.search(eq(INDEX), any(), eq(true))).thenReturn(response(hits("a", "b", "c"), "scroll-1"));
        when(client.search("scroll-1")).thenReturn(response(hits("d", "e"), "scroll-2"));

        final List<String> found = ids(searcher.search(INDEX, request(), NO_PARAMETERS, 0, Query.NO_LIMIT));

        assertEquals(ids(hits("a", "b", "c", "d", "e").stream()), found);
        assertEquals(PAGE_SIZE + 1, onlySearchBody(false).get("size"));
        final Map<String, Object> scrollBody = onlySearchBody(true);
        assertEquals(PAGE_SIZE, scrollBody.get("size"));
        assertEquals(0, scrollBody.get("from"));
        assertNull(scrollBody.get("track_total_hits"), "a scroll cannot switch off the total, Elasticsearch rejects that");
        verify(client).deleteScroll("scroll-2");
    }

    //A scroll cannot start at an offset, so once it takes over the offset is skipped on the client
    @Test
    public void shouldApplyTheOffsetOfAScrolledResultOnTheClient() throws IOException {
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(hits("e", "f", "g", "h"), null));
        when(client.search(eq(INDEX), any(), eq(true))).thenReturn(response(hits("a", "b", "c"), "scroll-1"));
        when(client.search("scroll-1")).thenReturn(response(hits("d", "e"), "scroll-2"));

        assertEquals(ids(hits("e").stream()), ids(searcher.search(INDEX, request(), NO_PARAMETERS, 4, Query.NO_LIMIT)));

        assertEquals(4, onlySearchBody(false).get("from"));
        assertEquals(0, onlySearchBody(true).get("from"));
    }

    //An offset at or beyond the last hit a request may return leaves nothing to ask for first, so the search scrolls
    //at once and skips the offset on the client
    @Test
    public void shouldScrollAtOnceWhenTheOffsetLeavesNoRoomForTheFirstRequest() throws IOException {
        final int offset = ElasticSearchSearcher.MAX_SEARCH_SIZE;
        when(client.search(eq(INDEX), any(), eq(true))).thenReturn(response(hits("a", "b"), "scroll-1"));

        assertEquals(0, searcher.search(INDEX, request(), NO_PARAMETERS, offset, Query.NO_LIMIT).count());

        verify(client, never()).search(anyString(), any(), eq(false));
        assertEquals(0, onlySearchBody(true).get("from"));
        verify(client).deleteScroll("scroll-1");
    }

    //A limit beyond what one request may return is served like an unlimited result, and the scroll stops at it
    @Test
    public void shouldScrollALimitBeyondTheLargestSingleRequest() throws IOException {
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(hits("a", "b", "c", "d"), null));
        when(client.search(eq(INDEX), any(), eq(true))).thenReturn(response(hits("a", "b", "c"), "scroll-1"));
        when(client.search("scroll-1")).thenReturn(response(hits("d", "e", "f"), "scroll-2"));
        when(client.search("scroll-2")).thenReturn(response(hits(), "scroll-3"));

        final List<String> found = ids(searcher.search(INDEX, request(), NO_PARAMETERS, 1,
            ElasticSearchSearcher.MAX_SEARCH_SIZE));

        assertEquals(ids(hits("b", "c", "d", "e", "f").stream()), found);
        assertEquals(PAGE_SIZE + 1, onlySearchBody(false).get("size"));
        assertEquals(PAGE_SIZE, onlySearchBody(true).get("size"));
    }

    //A limit too large for one request still bounds the scroll: the pages beyond it are never requested
    @Test
    public void shouldStopTheScrollAtTheLimitAndReleaseIt() throws IOException {
        final int pageSize = ElasticSearchSearcher.MAX_SEARCH_SIZE;
        searcher = searcher(pageSize, ElasticSearchPagingMode.POINT_IN_TIME);
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(page(0, pageSize), null));
        when(client.search(eq(INDEX), any(), eq(true))).thenReturn(response(page(0, pageSize), "scroll-1"));
        when(client.search("scroll-1")).thenReturn(response(page(pageSize, pageSize), "scroll-2"));

        final List<String> found = ids(searcher.search(INDEX, request(), NO_PARAMETERS, 0, pageSize + 2));

        assertEquals(pageSize + 2, found.size());
        assertEquals(ids(page(0, pageSize + 2).stream()), found);
        verify(client, never()).search("scroll-2");
        verify(client).deleteScroll("scroll-2");
    }

    //The first request never reaches beyond the last hit a request may return, so a page as large as that is asked
    //for whole, and a result which stays below it needs no scroll
    @Test
    public void shouldAskForTheWholeWindowWhenAPageIsAsLargeAsIt() throws IOException {
        searcher = searcher(ElasticSearchSearcher.MAX_SEARCH_SIZE, ElasticSearchPagingMode.POINT_IN_TIME);
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(hits("a", "b"), null));

        assertEquals(ids(hits("a", "b").stream()), ids(searcher.search(INDEX, request(), NO_PARAMETERS, 0, Query.NO_LIMIT)));

        assertEquals(ElasticSearchSearcher.MAX_SEARCH_SIZE, onlySearchBody(false).get("size"));
        verifyNoScroll();
    }

    //After a large offset the first request asks only for what is left of the window, and a result which stays
    //below that needs no scroll either
    @Test
    public void shouldAskForWhatIsLeftOfTheWindowAfterALargeOffset() throws IOException {
        final int offset = ElasticSearchSearcher.MAX_SEARCH_SIZE - 2;
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(hits("y"), null));

        assertEquals(Collections.singletonList("y"), ids(searcher.search(INDEX, request(), NO_PARAMETERS, offset, Query.NO_LIMIT)));

        final Map<String, Object> body = onlySearchBody(false);
        assertEquals(offset, body.get("from"));
        assertEquals(2, body.get("size"));
        verifyNoScroll();
    }

    //Closing the stream is how a consumer which stops early hands the scroll context back
    @Test
    public void shouldReleaseTheScrollWhenTheStreamIsClosed() throws IOException {
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(hits("a", "b", "c", "d"), null));
        when(client.search(eq(INDEX), any(), eq(true))).thenReturn(response(hits("a", "b", "c"), "scroll-1"));

        try (Stream<RawQuery.Result<String>> results = searcher.search(INDEX, request(), NO_PARAMETERS, 0, Query.NO_LIMIT)) {
            assertEquals("a", results.iterator().next().getResult());
            verify(client, never()).deleteScroll(anyString());
        }

        verify(client).deleteScroll("scroll-1");
        verify(client, never()).search("scroll-1");
    }

    @Test
    public void shouldNotOpenAScrollForAResultWhichNeededNone() throws IOException {
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(hits("a"), null));

        try (Stream<RawQuery.Result<String>> results = searcher.search(INDEX, request(), NO_PARAMETERS, 0, Query.NO_LIMIT)) {
            assertEquals(1, results.count());
        }

        verifyNoScroll();
    }

    //The scroll hands out the offset on top of the limit, and a finite sum is kept whole, however large
    @Test
    public void shouldRepresentTheWholeFiniteScrollLimit() {
        assertEquals(7L, ElasticSearchSearcher.scrollLimit(3, 4));
        assertEquals(Long.MAX_VALUE, ElasticSearchSearcher.scrollLimit(3, Query.NO_LIMIT));
        assertEquals(4_294_967_293L, ElasticSearchSearcher.scrollLimit(Integer.MAX_VALUE, Integer.MAX_VALUE - 1));
    }

    @Test
    public void shouldKeepTheScoresOfTheHits() throws IOException {
        final List<RawQuery.Result<String>> hits = Collections.singletonList(new RawQuery.Result<>("a", 2.5));
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(hits, null));

        final List<RawQuery.Result<String>> found = searcher.search(INDEX, request(), NO_PARAMETERS, 0, 1)
            .collect(Collectors.toList());

        assertEquals(1, found.size());
        assertEquals(2.5, found.get(0).getScore(), 0.0);
    }

    private static ElasticSearchRequest request() {
        final ElasticSearchRequest request = new ElasticSearchRequest();
        request.setQuery(Collections.singletonMap("match_all", Collections.emptyMap()));
        request.setDisableSourceRetrieval(true);
        return request;
    }

    private static List<RawQuery.Result<String>> hits(String... ids) {
        final List<RawQuery.Result<String>> hits = new ArrayList<>(ids.length);
        for (String id : ids) {
            hits.add(new RawQuery.Result<>(id, 1.0));
        }
        return hits;
    }

    private static List<RawQuery.Result<String>> page(int firstId, int size) {
        return hits(IntStream.range(firstId, firstId + size).mapToObj(Integer::toString).toArray(String[]::new));
    }

    private static ElasticSearchResponse response(List<RawQuery.Result<String>> hits, String scrollId) {
        final ElasticSearchResponse response = new ElasticSearchResponse();
        response.setResults(hits);
        response.setScrollId(scrollId);
        return response;
    }

    private static List<String> ids(Stream<RawQuery.Result<String>> results) {
        return results.map(RawQuery.Result::getResult).collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> onlySearchBody(boolean scroll) throws IOException {
        final ArgumentCaptor<Map<String, Object>> body = ArgumentCaptor.forClass(Map.class);
        verify(client, times(1)).search(eq(INDEX), body.capture(), eq(scroll));
        return body.getValue();
    }

    private void verifyNoScroll() throws IOException {
        verify(client, never()).search(anyString(), any(), eq(true));
        verify(client, never()).search(anyString());
        verify(client, never()).deleteScroll(anyString());
    }

    /* ---------------------------------------------------------------
     * In point in time mode a cluster with points in time reads the pages of a result through one, never through a scroll
     * ---------------------------------------------------------------
     */

    private static ElasticSearchResponse pitPage(List<RawQuery.Result<String>> hits, String pitId) {
        final ElasticSearchResponse response = new ElasticSearchResponse();
        response.setResults(hits);
        response.setPitId(pitId);
        response.setLastSort(hits.isEmpty() ? null : Arrays.asList(1f, hits.size()));
        return response;
    }

    //The probe comes back full: the result is larger than a page, and the point in time pit-1 opens
    private void givenAResultLargerThanAPageOnAClusterWithPointsInTime() throws IOException {
        when(client.supportsPointInTime()).thenReturn(true);
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(hits("a", "b", "c", "d"), null));
        when(client.openPointInTime(INDEX)).thenReturn("pit-1");
    }

    @Test
    public void shouldPageThroughAPointInTimeWhereTheClusterHasOne() throws IOException {
        givenAResultLargerThanAPageOnAClusterWithPointsInTime();
        when(client.searchPointInTime(eq("pit-1"), any()))
            .thenReturn(pitPage(hits("a", "b", "c"), "pit-1"), pitPage(hits("d", "e"), "pit-1"));

        final List<String> found = ids(searcher.search(INDEX, request(), NO_PARAMETERS, 0, Query.NO_LIMIT));

        assertEquals(Arrays.asList("a", "b", "c", "d", "e"), found);
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Map<String, Object>> body = ArgumentCaptor.forClass(Map.class);
        verify(client, times(2)).searchPointInTime(eq("pit-1"), body.capture());
        //Both pages share the body: a page of the page size, no offset, no count of the total, sorted in the order of
        //the index where the query doesn't sort, and the second asked for after the last hit of the first
        final Map<String, Object> pageBody = body.getValue();
        assertEquals(PAGE_SIZE, pageBody.get("size"));
        assertNull(pageBody.get("from"));
        assertEquals(false, pageBody.get("track_total_hits"));
        assertEquals(Collections.singletonList(Collections.singletonMap("_shard_doc", "asc")), pageBody.get("sort"));
        assertEquals(Arrays.asList(1f, 3), pageBody.get("search_after"));
        verify(client).closePointInTime("pit-1");
        verifyNoScroll();
    }

    //A limited query may be executed again with a larger limit, skipping the hits delivered so far, so its pages keep
    //the order of its single request, by score; only a query without a limit pages in the order of the index
    @Test
    public void shouldPageALimitedResultByScore() throws IOException {
        when(client.supportsPointInTime()).thenReturn(true);
        //Offset 9,999 and limit 2 reach beyond the first 10,000 hits: the probe may ask for one hit, and gets it
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(hits("x"), null));
        when(client.openPointInTime(INDEX)).thenReturn("pit-1");
        when(client.searchPointInTime(eq("pit-1"), any())).thenReturn(pitPage(page(0, 2_500), "pit-1"));

        //Pages of 2,500: five hold the 10,001 hits of offset and limit
        searcher = searcher(2_500, ElasticSearchPagingMode.POINT_IN_TIME);
        assertEquals(2, searcher.search(INDEX, request(), NO_PARAMETERS, 9_999, 2).count());

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Map<String, Object>> body = ArgumentCaptor.forClass(Map.class);
        verify(client, times(5)).searchPointInTime(eq("pit-1"), body.capture());
        assertEquals(Collections.singletonList(Collections.singletonMap("_score", "desc")), body.getValue().get("sort"));
    }

    //A sort given as a parameter of a raw query is the query's own
    @Test
    public void shouldKeepASortGivenAsAParameter() throws IOException {
        givenAResultLargerThanAPageOnAClusterWithPointsInTime();
        when(client.searchPointInTime(eq("pit-1"), any())).thenReturn(pitPage(hits("a", "b"), "pit-1"));
        final Parameter[] parameters = {new Parameter<>("sort", Collections.singletonList(Collections.singletonMap("time", "asc")))};

        assertEquals(2, searcher.search(INDEX, request(), parameters, 0, Query.NO_LIMIT).count());

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Map<String, Object>> body = ArgumentCaptor.forClass(Map.class);
        verify(client).searchPointInTime(eq("pit-1"), body.capture());
        assertEquals(Collections.singletonList(Collections.singletonMap("time", "asc")), body.getValue().get("sort"));
    }

    //A sort may be given as a field name or as an object as well as a list; any of them is kept
    @Test
    public void shouldKeepASortGivenAsAFieldNameOrAnObject() throws IOException {
        for (final Object sort : Arrays.asList("time", Collections.singletonMap("time", "asc"))) {
            setUp();
            givenAResultLargerThanAPageOnAClusterWithPointsInTime();
            when(client.searchPointInTime(eq("pit-1"), any())).thenReturn(pitPage(hits("a", "b"), "pit-1"));
            final Parameter[] parameters = {new Parameter<>("sort", sort)};

            assertEquals(2, searcher.search(INDEX, request(), parameters, 0, Query.NO_LIMIT).count());

            @SuppressWarnings("unchecked")
            final ArgumentCaptor<Map<String, Object>> body = ArgumentCaptor.forClass(Map.class);
            verify(client).searchPointInTime(eq("pit-1"), body.capture());
            assertEquals(sort, body.getValue().get("sort"), String.valueOf(sort));
        }
    }

    //A raw query's hits come in the order of their scores, which the pages of a point in time keep
    @Test
    public void shouldPageARelevanceOrderedResultByScore() throws IOException {
        givenAResultLargerThanAPageOnAClusterWithPointsInTime();
        when(client.searchPointInTime(eq("pit-1"), any())).thenReturn(pitPage(hits("a", "b"), "pit-1"));
        final ElasticSearchRequest request = request();
        request.setRelevanceOrdered(true);

        assertEquals(2, searcher.search(INDEX, request, NO_PARAMETERS, 0, Query.NO_LIMIT).count());

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Map<String, Object>> body = ArgumentCaptor.forClass(Map.class);
        verify(client).searchPointInTime(eq("pit-1"), body.capture());
        assertEquals(Collections.singletonList(Collections.singletonMap("_score", "desc")), body.getValue().get("sort"));
    }

    @Test
    public void shouldKeepTheQuerysSortWhenPagingThroughAPointInTime() throws IOException {
        givenAResultLargerThanAPageOnAClusterWithPointsInTime();
        when(client.searchPointInTime(eq("pit-1"), any())).thenReturn(pitPage(hits("a", "b"), "pit-1"));
        final ElasticSearchRequest request = request();
        request.addSort("time", "asc", "long");

        assertEquals(2, searcher.search(INDEX, request, NO_PARAMETERS, 0, Query.NO_LIMIT).count());

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Map<String, Object>> body = ArgumentCaptor.forClass(Map.class);
        verify(client).searchPointInTime(eq("pit-1"), body.capture());
        assertEquals(request.getSorts(), body.getValue().get("sort"));
    }

    @Test
    public void shouldSkipTheOffsetOfAPointInTimeOnTheClient() throws IOException {
        when(client.supportsPointInTime()).thenReturn(true);
        //The probe asks for a page and one hit after the offset of 2, and comes back full
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(hits("c", "d", "e", "f"), null));
        when(client.openPointInTime(INDEX)).thenReturn("pit-1");
        when(client.searchPointInTime(eq("pit-1"), any())).thenReturn(pitPage(hits("a", "b", "c"), "pit-1"),
            pitPage(hits("d", "e", "f"), "pit-1"), pitPage(hits("g"), "pit-1"));

        //The pages start at the first hit; the offset is skipped here
        assertEquals(Arrays.asList("c", "d", "e", "f", "g"),
            ids(searcher.search(INDEX, request(), NO_PARAMETERS, 2, Query.NO_LIMIT)));

        verify(client, times(3)).searchPointInTime(eq("pit-1"), any());
        verify(client).closePointInTime("pit-1");
    }

    //A limit within the first 10,000 hits is one request; beyond them the pages stop once they hold offset and limit
    @Test
    public void shouldStopThePagesOfAPointInTimeAtTheLimit() throws IOException {
        when(client.supportsPointInTime()).thenReturn(true);
        //Offset 9,999: the probe may ask for one hit only, and it comes back
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(response(hits("x"), null));
        when(client.openPointInTime(INDEX)).thenReturn("pit-1");
        when(client.searchPointInTime(eq("pit-1"), any())).thenReturn(pitPage(page(0, 2_500), "pit-1"),
            pitPage(page(2_500, 2_500), "pit-1"), pitPage(page(5_000, 2_500), "pit-1"),
            pitPage(page(7_500, 2_500), "pit-1"), pitPage(page(10_000, 2_500), "pit-1"));

        searcher = searcher(2_500, ElasticSearchPagingMode.POINT_IN_TIME);
        assertEquals(Arrays.asList("9999", "10000"), ids(searcher.search(INDEX, request(), NO_PARAMETERS, 9_999, 2)));

        //Five pages of 2,500 hold the 10,001 hits of offset and limit; none beyond them is asked for
        verify(client, times(5)).searchPointInTime(eq("pit-1"), any());
        verify(client).closePointInTime("pit-1");
    }

    @Test
    public void shouldClosePointInTimeWhenTheFirstPageFails() throws IOException {
        givenAResultLargerThanAPageOnAClusterWithPointsInTime();
        when(client.searchPointInTime(eq("pit-1"), any())).thenThrow(new IOException("search rejected"));

        assertThrows(IOException.class, () -> searcher.search(INDEX, request(), NO_PARAMETERS, 0, Query.NO_LIMIT));

        verify(client).closePointInTime("pit-1");
    }

    @Test
    public void shouldReleaseThePointInTimeWhenTheStreamIsClosed() throws IOException {
        givenAResultLargerThanAPageOnAClusterWithPointsInTime();
        when(client.searchPointInTime(eq("pit-1"), any())).thenReturn(pitPage(hits("a", "b", "c"), "pit-1"));

        try (Stream<RawQuery.Result<String>> hits = searcher.search(INDEX, request(), NO_PARAMETERS, 0, Query.NO_LIMIT)) {
            assertEquals("a", hits.iterator().next().getResult());
        }

        verify(client).closePointInTime("pit-1");
        verify(client, times(1)).searchPointInTime(any(), any());
    }

    /* ---------------------------------------------------------------
     * The paging mode decides which results a cluster with points in time reads through one; the others scroll
     * ---------------------------------------------------------------
     */

    //The probe comes back full from an index of the shards given, on a cluster which can read the pages through a point
    //in time or a scroll; either reads the hits a and b
    private void givenAResultLargerThanAPageOfAnIndexOf(int shards) throws IOException {
        when(client.supportsPointInTime()).thenReturn(true);
        final ElasticSearchResponse probe = response(hits("a", "b", "c", "d"), null);
        probe.setTotalShards(shards);
        when(client.search(eq(INDEX), any(), eq(false))).thenReturn(probe);
        when(client.openPointInTime(INDEX)).thenReturn("pit-1");
        when(client.searchPointInTime(eq("pit-1"), any())).thenReturn(pitPage(hits("a", "b"), "pit-1"));
        when(client.search(eq(INDEX), any(), eq(true))).thenReturn(response(hits("a", "b"), "scroll-1"));
    }

    private void assertScrolled(ElasticSearchRequest request, Parameter[] parameters, int limit) throws IOException {
        assertEquals(Arrays.asList("a", "b"), ids(searcher.search(INDEX, request, parameters, 0, limit)));
        verify(client).search(eq(INDEX), any(), eq(true));
        verify(client, never()).openPointInTime(anyString());
    }

    private void assertReadThroughAPointInTimeInTheOrderOfTheIndex() throws IOException {
        assertEquals(Arrays.asList("a", "b"),
            ids(searcher.search(INDEX, request(), NO_PARAMETERS, 0, Query.NO_LIMIT)));
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Map<String, Object>> body = ArgumentCaptor.forClass(Map.class);
        verify(client).searchPointInTime(eq("pit-1"), body.capture());
        assertEquals(Collections.singletonList(Collections.singletonMap("_shard_doc", "asc")),
            body.getValue().get("sort"));
        verify(client).closePointInTime("pit-1");
        verify(client, never()).search(anyString(), any(), eq(true));
    }

    private ElasticSearchSearcher adaptiveSearcher(int maxShards, int maxPageSize) {
        return new ElasticSearchSearcher(client, new ES7Compat(), PAGE_SIZE,
            ElasticSearchPagingMode.ADAPTIVE_POINT_IN_TIME, maxShards, maxPageSize);
    }

    @Test
    public void shouldScrollInScrollModeWhereTheClusterHasPointsInTime() throws IOException {
        givenAResultLargerThanAPageOfAnIndexOf(1);
        searcher = searcher(PAGE_SIZE, ElasticSearchPagingMode.SCROLL);
        assertScrolled(request(), NO_PARAMETERS, Query.NO_LIMIT);
    }

    //Small pages of one shard in the order of the index come as fast through a point in time as through a scroll
    @Test
    public void shouldReadAResultInTheOrderOfTheIndexThroughAPointInTimeInAdaptiveMode() throws IOException {
        givenAResultLargerThanAPageOfAnIndexOf(1);
        searcher = adaptiveSearcher(1, PAGE_SIZE);
        assertReadThroughAPointInTimeInTheOrderOfTheIndex();
    }

    @Test
    public void shouldReadAnIndexOfAsManyShardsAsItsBoundThroughAPointInTimeInAdaptiveMode() throws IOException {
        givenAResultLargerThanAPageOfAnIndexOf(2);
        searcher = adaptiveSearcher(2, 500);
        assertReadThroughAPointInTimeInTheOrderOfTheIndex();
    }

    //A limited query may be executed again with a larger limit, so a point in time would read its pages by score
    @Test
    public void shouldScrollALimitedResultInAdaptiveMode() throws IOException {
        givenAResultLargerThanAPageOfAnIndexOf(1);
        searcher = adaptiveSearcher(1, 500);
        assertScrolled(request(), NO_PARAMETERS, ElasticSearchSearcher.MAX_SEARCH_SIZE + 1);
    }

    @Test
    public void shouldScrollARelevanceOrderedResultInAdaptiveMode() throws IOException {
        givenAResultLargerThanAPageOfAnIndexOf(1);
        searcher = adaptiveSearcher(1, 500);
        final ElasticSearchRequest request = request();
        request.setRelevanceOrdered(true);
        assertScrolled(request, NO_PARAMETERS, Query.NO_LIMIT);
    }

    //Whether the query sorts or a parameter does
    @Test
    public void shouldScrollASortedResultInAdaptiveMode() throws IOException {
        givenAResultLargerThanAPageOfAnIndexOf(1);
        searcher = adaptiveSearcher(1, 500);
        final ElasticSearchRequest request = request();
        request.addSort("time", "asc", "long");
        assertScrolled(request, NO_PARAMETERS, Query.NO_LIMIT);

        setUp();
        givenAResultLargerThanAPageOfAnIndexOf(1);
        searcher = adaptiveSearcher(1, 500);
        assertScrolled(request(), new Parameter[]{new Parameter<>("sort", "time")}, Query.NO_LIMIT);
    }

    //A response which doesn't tell its shards may come from any number of them
    @Test
    public void shouldScrollAnIndexOfMoreShardsThanItsBoundInAdaptiveMode() throws IOException {
        for (final int shards : new int[]{2, 0}) {
            setUp();
            givenAResultLargerThanAPageOfAnIndexOf(shards);
            searcher = adaptiveSearcher(1, 500);
            assertScrolled(request(), NO_PARAMETERS, Query.NO_LIMIT);
        }
    }

    //An offset which leaves no room for the first request leaves the shards untold, so the search scrolls at once
    @Test
    public void shouldScrollInAdaptiveModeWhenTheOffsetLeavesNoRoomForTheFirstRequest() throws IOException {
        givenAResultLargerThanAPageOfAnIndexOf(1);
        searcher = adaptiveSearcher(1, 500);

        assertEquals(0, searcher.search(INDEX, request(), NO_PARAMETERS, ElasticSearchSearcher.MAX_SEARCH_SIZE,
            Query.NO_LIMIT).count());

        verify(client, never()).search(anyString(), any(), eq(false));
        verify(client).search(eq(INDEX), any(), eq(true));
        verify(client, never()).openPointInTime(anyString());
    }

    @Test
    public void shouldScrollInAdaptiveModeWhereTheClusterHasNoPointsInTime() throws IOException {
        givenAResultLargerThanAPageOfAnIndexOf(1);
        when(client.supportsPointInTime()).thenReturn(false);
        searcher = adaptiveSearcher(1, 500);
        assertScrolled(request(), NO_PARAMETERS, Query.NO_LIMIT);
    }

    @Test
    public void shouldScrollPagesLargerThanTheirBoundInAdaptiveMode() throws IOException {
        givenAResultLargerThanAPageOfAnIndexOf(1);
        searcher = adaptiveSearcher(1, PAGE_SIZE - 1);
        assertScrolled(request(), NO_PARAMETERS, Query.NO_LIMIT);
    }
}
