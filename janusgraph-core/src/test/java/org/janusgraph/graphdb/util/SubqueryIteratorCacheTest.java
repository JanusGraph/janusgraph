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

package org.janusgraph.graphdb.util;

import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.query.Query;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.transaction.subquerycache.SubqueryCache;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

//The subquery cache is consulted for the results of one index of a joint query. It stores a result list against the
//limit of the subquery which produced it, and serves that list only to a later query whose limit is no larger. So a
//result list which the read limit truncated is safe to store only while the subquery carries that same limit.
//JointIndexQuery.updateLimit propagates the joint limit into the subquery only when there is a single subquery. With
//more than one the subquery keeps a wider limit, and a truncated list stored against it is served to a later query
//which asked for more results than the list holds.
public class SubqueryIteratorCacheTest {

    private static final List<Object> ALL_MATCHING_IDS = Arrays.asList(1L, 2L, 3L, 4L, 5L);

    private final JointIndexQuery.Subquery subQuery = mock(JointIndexQuery.Subquery.class);
    private final SubqueryCache indexCache = mock(SubqueryCache.class);

    //readLimit is the limit the iterator reads the index under, which a joint query takes from itself. subQueryLimit is
    //the limit the cache records the result against, which it takes from the subquery.
    private void runQuery(int readLimit, int subQueryLimit) {
        when(subQuery.getProfiler()).thenReturn(QueryProfiler.NO_OP);
        when(subQuery.getLimit()).thenReturn(subQueryLimit);
        final IndexSerializer indexSerializer = mock(IndexSerializer.class);
        when(indexSerializer.query(any(), any(), any())).thenReturn(ALL_MATCHING_IDS.stream());
        //A mock returns an empty List rather than null, which would look like a cache hit holding no results
        when(indexCache.getIfPresent(any())).thenReturn(null);

        try (SubqueryIterator iterator = new SubqueryIterator(subQuery, indexSerializer,
            mock(BackendTransaction.class), mock(StandardJanusGraphTx.class), indexCache, readLimit,
            id -> mock(JanusGraphElement.class), null)) {
            iterator.forEachRemaining(element -> { });
        }
    }

    private List<Object> captureCachedResult() {
        final ArgumentCaptor<List<Object>> cached = ArgumentCaptor.forClass(List.class);
        verify(indexCache, times(1)).put(any(), cached.capture());
        return cached.getValue();
    }

    @Test
    public void shouldNotCacheATruncatedResultSetWhenTheSubqueryLimitIsWider() {
        //This is the joint query of more than one index: the read stopped after 2 results, but the cache would record
        //the list against an unlimited subquery and then serve those 2 results to a query which asked for all of them
        runQuery(2, Query.NO_LIMIT);
        verify(indexCache, never()).put(any(), any());
    }

    @Test
    public void shouldCacheATruncatedResultSetWhenTheSubqueryCarriesTheSameLimit() {
        //This is the joint query of a single index: the cache records the same limit which truncated the read, so it
        //declines to serve the list to a later query which asks for more
        runQuery(2, 2);
        assertEquals(ALL_MATCHING_IDS.subList(0, 2), captureCachedResult());
    }

    @Test
    public void shouldCacheAResultSetWhoseIndexRanOutOfResults() {
        runQuery(ALL_MATCHING_IDS.size() + 1, ALL_MATCHING_IDS.size() + 1);
        assertEquals(ALL_MATCHING_IDS, captureCachedResult());
    }

    @Test
    public void shouldCacheAResultSetWhenThereIsNoLimit() {
        runQuery(Query.NO_LIMIT, Query.NO_LIMIT);
        assertEquals(ALL_MATCHING_IDS, captureCachedResult());
    }

    @Test
    public void shouldCacheAResultSetWhoseIndexRanOutAsTheLimitWasReached() {
        //The index ran out at the same moment the limit was reached. Which of the two stopped the read is unknown, so
        //the list counts as truncated, and the subquery carrying the same limit is what makes it safe to store
        runQuery(ALL_MATCHING_IDS.size(), ALL_MATCHING_IDS.size());
        assertEquals(ALL_MATCHING_IDS, captureCachedResult());
    }

    @Test
    public void shouldNotCacheAResultSetWhoseIndexRanOutAsAWiderSubqueryLimitWasReached() {
        runQuery(ALL_MATCHING_IDS.size(), Query.NO_LIMIT);
        verify(indexCache, never()).put(any(), any());
    }

    @Test
    public void shouldCacheAnEmptyResultSet() {
        when(subQuery.getProfiler()).thenReturn(QueryProfiler.NO_OP);
        final IndexSerializer indexSerializer = mock(IndexSerializer.class);
        when(indexSerializer.query(any(), any(), any())).thenReturn(Collections.emptyList().stream());
        when(indexCache.getIfPresent(any())).thenReturn(null);

        try (SubqueryIterator iterator = new SubqueryIterator(subQuery, indexSerializer,
            mock(BackendTransaction.class), mock(StandardJanusGraphTx.class), indexCache, 10,
            id -> mock(JanusGraphElement.class), null)) {
            iterator.forEachRemaining(element -> { });
        }

        //An index which matched nothing is a complete answer, and worth caching
        assertEquals(Collections.emptyList(), captureCachedResult());
    }
}
