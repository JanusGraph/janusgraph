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
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.transaction.subquerycache.SubqueryCache;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

//When a query is covered by more than one index, StandardJanusGraphTx intersects the results: the first index is
//streamed, and every element is checked for membership in the results the other indexes agreed on. That other set is
//deliberately unbounded, because NO_LIMIT is passed to processIntersectingRetrievals to keep the intersection
//complete, so the membership check has to be cheap.
public class SubqueryIteratorTest {

    private static final long ID_ONLY_IN_FIRST_INDEX = 1L;
    private static final long ID_IN_EVERY_INDEX = 2L;
    private static final long ID_ONLY_IN_OTHER_INDEXES = 99L;

    private List<Object> streamedIds(List<Object> firstIndexResults, List<Object> otherResults, int limit) {
        final JointIndexQuery.Subquery subQuery = mock(JointIndexQuery.Subquery.class);
        when(subQuery.getProfiler()).thenReturn(QueryProfiler.NO_OP);

        final IndexSerializer indexSerializer = mock(IndexSerializer.class);
        when(indexSerializer.query(any(), any(), any())).thenReturn(firstIndexResults.stream());

        //A mock returns an empty List rather than null, which would look like a cache hit holding no results
        final SubqueryCache indexCache = mock(SubqueryCache.class);
        when(indexCache.getIfPresent(any())).thenReturn(null);

        final List<Object> returnedIds = new ArrayList<>();
        try (SubqueryIterator iterator = new SubqueryIterator(subQuery, indexSerializer,
            mock(BackendTransaction.class), mock(StandardJanusGraphTx.class), indexCache, limit,
            id -> {
                //The conversion function turns an id into an element. Its identity does not matter here, only which
                //ids reach it
                returnedIds.add(id);
                return mock(JanusGraphElement.class);
            }, otherResults)) {
            iterator.forEachRemaining(element -> { });
        }
        return returnedIds;
    }

    @Test
    public void shouldKeepOnlyTheIdsPresentInTheOtherIndexResults() {
        final List<Object> streamed = streamedIds(
            Arrays.asList(ID_ONLY_IN_FIRST_INDEX, ID_IN_EVERY_INDEX),
            Arrays.asList(ID_IN_EVERY_INDEX, ID_ONLY_IN_OTHER_INDEXES),
            Integer.MAX_VALUE);
        assertEquals(Collections.singletonList(ID_IN_EVERY_INDEX), streamed);
    }

    @Test
    public void shouldKeepEveryIdWhenThereIsNoOtherIndex() {
        //A single index query passes null, which must not be treated as an empty intersection
        final List<Object> firstIndexResults = Arrays.asList(ID_ONLY_IN_FIRST_INDEX, ID_IN_EVERY_INDEX);
        assertEquals(firstIndexResults, streamedIds(firstIndexResults, null, Integer.MAX_VALUE));
    }

    @Test
    public void shouldKeepNoIdWhenTheOtherIndexesAgreedOnNothing() {
        assertEquals(Collections.emptyList(), streamedIds(
            Arrays.asList(ID_ONLY_IN_FIRST_INDEX, ID_IN_EVERY_INDEX), Collections.emptyList(), Integer.MAX_VALUE));
    }

    @Test
    public void shouldStopAtTheLimit() {
        final List<Object> otherResults = Arrays.asList(ID_ONLY_IN_FIRST_INDEX, ID_IN_EVERY_INDEX);
        assertEquals(Collections.singletonList(ID_ONLY_IN_FIRST_INDEX), streamedIds(
            Arrays.asList(ID_ONLY_IN_FIRST_INDEX, ID_IN_EVERY_INDEX), otherResults, 1));
    }

    @Test
    public void shouldNotBeConfusedByADuplicateInTheOtherIndexResults() {
        //processIntersectingRetrievals returns a List, so a repeated id is possible. Membership is all that matters
        assertEquals(Collections.singletonList(ID_IN_EVERY_INDEX), streamedIds(
            Collections.singletonList(ID_IN_EVERY_INDEX),
            Arrays.asList(ID_IN_EVERY_INDEX, ID_IN_EVERY_INDEX), Integer.MAX_VALUE));
    }

    @Test
    public void shouldNotDependOnStreamOrderMatchingTheOtherIndexOrder() {
        //The streamed order is preserved, and is independent of the order the other indexes reported
        assertEquals(Arrays.asList(ID_IN_EVERY_INDEX, ID_ONLY_IN_FIRST_INDEX), streamedIds(
            Arrays.asList(ID_IN_EVERY_INDEX, ID_ONLY_IN_FIRST_INDEX),
            Arrays.asList(ID_ONLY_IN_FIRST_INDEX, ID_ONLY_IN_OTHER_INDEXES, ID_IN_EVERY_INDEX),
            Integer.MAX_VALUE));
    }
}
