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

import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.query.Query;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.transaction.subquerycache.CaffeineSubqueryCache;
import org.janusgraph.graphdb.util.SubqueryIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A scroll, or the pages of a point in time, whose later page fails, read as a graph query reads them. Closing the
 * transaction's iterator over an index stores the ids it read in the cache of index results where it finds that the
 * result ended, and the same query would then get them for the rest of the transaction. After a failure, the iterator
 * neither asks whether the result ended nor caches anything, and closing it, as closing the traversal does, doesn't
 * throw again over the failure which the read threw. The scroll and the point in time also fail every later call with
 * that failure, rather than answer that the result ended.
 */
public class ElasticSearchFailedPageCacheTest {

    private static final int PAGE_SIZE = 3;
    private static final List<String> FIRST_PAGE = Arrays.asList("doc0", "doc1", "doc2");

    private ElasticSearchClient client;

    @BeforeEach
    public void setUp() {
        client = mock(ElasticSearchClient.class);
    }

    @Test
    public void shouldNotCacheTheHitsReadBeforeAPageOfAScrollFailed() throws IOException {
        when(client.search("scroll-1")).thenThrow(new IOException(
            "Elasticsearch answered a page of a scroll with the results of only 1 of 2 shards"));
        final ElasticSearchResponse firstPage = firstPage();
        firstPage.setScrollId("scroll-1");
        final ElasticSearchScroll scroll = new ElasticSearchScroll(client, firstPage, PAGE_SIZE);

        assertNothingCached(scroll, scroll::close);
        verify(client, times(1)).search("scroll-1");
        verify(client, times(1)).deleteScroll("scroll-1");
    }

    @Test
    public void shouldNotCacheTheHitsReadBeforeAPageOfAPointInTimeFailed() throws IOException {
        when(client.searchPointInTime(eq("pit-1"), any())).thenThrow(new IOException(
            "Elasticsearch answered a page of a point in time with the results of only 1 of 2 shards"));
        final ElasticSearchResponse firstPage = firstPage();
        firstPage.setLastSort(Arrays.asList(1f, 2));
        final ElasticSearchPointInTime pages = new ElasticSearchPointInTime(client, "pit-1", new HashMap<>(),
            firstPage, PAGE_SIZE, Long.MAX_VALUE);

        assertNothingCached(pages, pages::close);
        verify(client, times(1)).searchPointInTime(any(), any());
        verify(client, times(1)).closePointInTime("pit-1");
    }

    private static ElasticSearchResponse firstPage() {
        final ElasticSearchResponse page = new ElasticSearchResponse();
        page.setResults(FIRST_PAGE.stream().map(id -> new RawQuery.Result<>(id, 1.0)).collect(Collectors.toList()));
        return page;
    }

    //Reads the hits as a graph query does, through the SubqueryIterator of a subquery without a limit, which turns each
    //id into an element and caches the ids of a complete result in a cache like the transaction's, and closes it after
    //the failure. The stream is built as ElasticSearchSearcher builds it
    private static void assertNothingCached(Iterator<RawQuery.Result<String>> hits, Runnable closeHits) {
        final Stream<String> fromIndex = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(hits, Spliterator.ORDERED), false)
            .onClose(closeHits).map(RawQuery.Result::getResult);
        final JointIndexQuery.Subquery subQuery = mock(JointIndexQuery.Subquery.class);
        when(subQuery.getProfiler()).thenReturn(QueryProfiler.NO_OP);
        when(subQuery.getLimit()).thenReturn(Query.NO_LIMIT);
        when(subQuery.updateLimit(0)).thenReturn(subQuery);
        final IndexSerializer serializer = mock(IndexSerializer.class);
        when(serializer.query(any(), any(), any())).thenAnswer(invocation -> fromIndex);
        final CaffeineSubqueryCache cache = new CaffeineSubqueryCache(10_000);

        final List<Object> read = new ArrayList<>();
        final SubqueryIterator iterator = new SubqueryIterator(subQuery, serializer, mock(BackendTransaction.class),
            mock(StandardJanusGraphTx.class), cache, Integer.MAX_VALUE, id -> {
                read.add(id);
                return mock(JanusGraphElement.class);
            }, null);
        assertThrows(UncheckedIOException.class, () -> iterator.forEachRemaining(element -> { }));
        assertEquals(FIRST_PAGE, read);
        iterator.close();
        assertNull(cache.getIfPresent(subQuery));
    }
}
