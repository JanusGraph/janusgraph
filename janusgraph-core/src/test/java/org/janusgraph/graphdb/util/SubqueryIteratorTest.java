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
import org.janusgraph.graphdb.query.LimitAdjustingIterator;
import org.janusgraph.graphdb.query.Query;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.transaction.subquerycache.SubqueryCache;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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
        //A Stream is consumed once, so answer with a fresh one rather than handing the same instance to every call
        when(indexSerializer.query(any(), any(), any())).thenAnswer(invocation -> firstIndexResults.stream());

        //A mock returns an empty List rather than null, which would look like a cache hit holding no results
        final SubqueryCache indexCache = mock(SubqueryCache.class);
        when(indexCache.getIfPresent(any())).thenReturn(null);
        //Unstubbed, maximumCachedResultSize() would return 0, which drops the ids at once: this stands for a cache
        //without a bound
        when(indexCache.maximumCachedResultSize()).thenReturn(Integer.MAX_VALUE);

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

    private SubqueryIterator iteratorOver(Stream<?> indexResults) {
        final JointIndexQuery.Subquery subQuery = mock(JointIndexQuery.Subquery.class);
        when(subQuery.getProfiler()).thenReturn(QueryProfiler.NO_OP);
        final IndexSerializer indexSerializer = mock(IndexSerializer.class);
        when(indexSerializer.query(any(), any(), any())).thenAnswer(invocation -> indexResults);
        final SubqueryCache indexCache = mock(SubqueryCache.class);
        when(indexCache.getIfPresent(any())).thenReturn(null);
        //Unstubbed, maximumCachedResultSize() would return 0, which drops the ids at once: this stands for a cache
        //without a bound
        when(indexCache.maximumCachedResultSize()).thenReturn(Integer.MAX_VALUE);
        return new SubqueryIterator(subQuery, indexSerializer, mock(BackendTransaction.class),
            mock(StandardJanusGraphTx.class), indexCache, Integer.MAX_VALUE, id -> mock(JanusGraphElement.class), null);
    }

    //An index backend may hold a resource behind its stream, such as an Elasticsearch scroll context, which
    //closing the stream hands back. A consumer which stops early only ever closes the iterator
    @Test
    public void shouldCloseTheIndexStreamWhenClosedBeforeTheEnd() {
        final AtomicBoolean closed = new AtomicBoolean();
        final SubqueryIterator iterator = iteratorOver(Stream.of(1L, 2L, 3L).onClose(() -> closed.set(true)));

        iterator.next();
        assertFalse(closed.get());
        iterator.close();

        assertTrue(closed.get());
    }

    @Test
    public void shouldCloseTheIndexStreamOnceExhausted() {
        final AtomicBoolean closed = new AtomicBoolean();
        final SubqueryIterator iterator = iteratorOver(Stream.of(1L, 2L).onClose(() -> closed.set(true)));

        iterator.forEachRemaining(element -> { });

        assertTrue(closed.get());
    }

    //An index stream which counts how often it is closed. A stream of the JDK runs its close handlers once however
    //often it is closed, which would hide a second close
    private static Stream<?> countingCloses(Stream<?> stream, AtomicInteger closes) {
        return (Stream<?>) Proxy.newProxyInstance(SubqueryIteratorTest.class.getClassLoader(),
            new Class<?>[]{Stream.class}, (proxy, method, args) -> {
                if ("close".equals(method.getName())) {
                    closes.incrementAndGet();
                }
                try {
                    return method.invoke(stream, args);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });
    }

    //Closing the iterator again, after it closed itself at its end or after its consumer closed it, closes the index
    //stream no second time, so that a resource behind it is handed back once
    @Test
    public void shouldCloseTheIndexStreamOnceHoweverOftenTheIteratorIsClosed() {
        final AtomicInteger closes = new AtomicInteger();
        final SubqueryIterator exhausted = iteratorOver(countingCloses(Stream.of(1L, 2L), closes));
        exhausted.forEachRemaining(element -> { });
        exhausted.close();
        assertEquals(1, closes.get());

        final SubqueryIterator closedEarly = iteratorOver(countingCloses(Stream.of(1L, 2L, 3L), closes));
        closedEarly.next();
        closedEarly.close();
        closedEarly.close();
        assertEquals(2, closes.get());
    }

    //An iterator over the ids of a subquery read under a limit, from the index or from the cache, of which the ids
    //without an element, as those of relations which the transaction removed, convert to null
    private SubqueryIterator limitedIterator(List<Object> ids, boolean fromCache, int limit, Object idWithoutElement) {
        final JointIndexQuery.Subquery subQuery = mock(JointIndexQuery.Subquery.class);
        when(subQuery.getProfiler()).thenReturn(QueryProfiler.NO_OP);
        when(subQuery.hasLimit()).thenReturn(true);
        when(subQuery.getLimit()).thenReturn(limit);
        final IndexSerializer indexSerializer = mock(IndexSerializer.class);
        when(indexSerializer.query(any(), any(), any())).thenAnswer(invocation -> ids.stream());
        final SubqueryCache indexCache = mock(SubqueryCache.class);
        when(indexCache.getIfPresent(any())).thenReturn(fromCache ? ids : null);
        when(indexCache.maximumCachedResultSize()).thenReturn(Integer.MAX_VALUE);
        return new SubqueryIterator(subQuery, indexSerializer, mock(BackendTransaction.class),
            mock(StandardJanusGraphTx.class), indexCache, limit,
            id -> id.equals(idWithoutElement) ? null : mock(JanusGraphElement.class), null);
    }

    private static int countElements(SubqueryIterator iterator) {
        int elements = 0;
        while (iterator.hasNext()) {
            iterator.next();
            elements++;
        }
        return elements;
    }

    //LimitAdjustingIterator reads on with a larger limit where the iterator comes to its end before the limit, but the
    //index returned as many ids as the limit allowed
    @Test
    public void shouldTellThatTheIndexReturnedAsManyIdsAsTheLimitAllowedThoughOneHadNoElement() {
        for (final boolean fromCache : new boolean[]{false, true}) {
            final SubqueryIterator iterator = limitedIterator(Arrays.asList(1L, 2L, 3L), fromCache, 3, 2L);
            assertEquals(2, countElements(iterator));
            assertTrue(iterator.hasBackendReachedLimit());
        }
    }

    @Test
    public void shouldTellThatTheIndexRanOutBeforeTheLimit() {
        for (final boolean fromCache : new boolean[]{false, true}) {
            final SubqueryIterator iterator = limitedIterator(Arrays.asList(1L, 2L), fromCache, 3, 2L);
            assertEquals(1, countElements(iterator));
            assertFalse(iterator.hasBackendReachedLimit());
        }
    }

    @Test
    public void shouldNotTellThatASubqueryWithoutALimitReachedIt() {
        final SubqueryIterator iterator = iteratorOver(Stream.of(1L, 2L, 3L));
        iterator.forEachRemaining(element -> { });
        assertFalse(iterator.hasBackendReachedLimit());
    }

    //A subquery read under a limit of five, whose conversion records the ids it is given
    private SubqueryIterator skippingIterator(JointIndexQuery.Subquery subQuery, SubqueryCache indexCache,
                                              List<Object> converted) {
        final IndexSerializer indexSerializer = mock(IndexSerializer.class);
        when(indexSerializer.query(any(), any(), any())).thenAnswer(invocation -> Stream.of(1L, 2L, 3L, 4L, 5L));
        return new SubqueryIterator(subQuery, indexSerializer, mock(BackendTransaction.class),
            mock(StandardJanusGraphTx.class), indexCache, 5, id -> {
                converted.add(id);
                return mock(JanusGraphElement.class);
            }, null);
    }

    private static JointIndexQuery.Subquery subqueryLimitedToFive() {
        final JointIndexQuery.Subquery subQuery = mock(JointIndexQuery.Subquery.class);
        when(subQuery.getProfiler()).thenReturn(QueryProfiler.NO_OP);
        when(subQuery.hasLimit()).thenReturn(true);
        when(subQuery.getLimit()).thenReturn(5);
        return subQuery;
    }

    private static SubqueryCache emptyCache() {
        final SubqueryCache indexCache = mock(SubqueryCache.class);
        when(indexCache.getIfPresent(any())).thenReturn(null);
        when(indexCache.maximumCachedResultSize()).thenReturn(Integer.MAX_VALUE);
        return indexCache;
    }

    //LimitAdjustingIterator has a read under a larger limit skip what the reads before went through
    @Test
    public void shouldSkipIdsWithoutTurningThemIntoElements() {
        final List<Object> converted = new ArrayList<>();
        final SubqueryIterator iterator = skippingIterator(subqueryLimitedToFive(), emptyCache(), converted);
        iterator.skipBackendResults(2);
        assertEquals(3, countElements(iterator));
        assertEquals(Arrays.asList(3L, 4L, 5L), converted);
        assertEquals(5, iterator.getBackendResultsRead());
        assertTrue(iterator.hasBackendReachedLimit());
    }

    //The ids collected for the cache are those of the subquery from its first, so the skipped ones are among them
    @Test
    public void shouldCollectTheSkippedIdsForTheCache() {
        final JointIndexQuery.Subquery subQuery = subqueryLimitedToFive();
        final SubqueryCache indexCache = emptyCache();
        final SubqueryIterator iterator = skippingIterator(subQuery, indexCache, new ArrayList<>());
        iterator.skipBackendResults(2);
        countElements(iterator);
        verify(indexCache).put(subQuery, Arrays.asList(1L, 2L, 3L, 4L, 5L));
    }

    //A read under a larger limit may find its ids in the cache, where an earlier query put them, and skips them alike
    @Test
    public void shouldSkipIdsFromTheCacheWithoutTurningThemIntoElements() {
        final SubqueryCache indexCache = mock(SubqueryCache.class);
        when(indexCache.getIfPresent(any())).thenReturn(Arrays.asList(11L, 12L, 13L, 14L, 15L));
        final List<Object> converted = new ArrayList<>();
        final SubqueryIterator iterator = skippingIterator(subqueryLimitedToFive(), indexCache, converted);
        iterator.skipBackendResults(2);
        assertEquals(3, countElements(iterator));
        assertEquals(Arrays.asList(13L, 14L, 15L), converted);
        assertEquals(5, iterator.getBackendResultsRead());
        assertTrue(iterator.hasBackendReachedLimit());
    }

    @Test
    public void shouldSkipOnlyACountOfAtLeastZeroBeforeTheIteratorLooksForItsFirstElement() {
        final SubqueryIterator iterator = skippingIterator(subqueryLimitedToFive(), emptyCache(), new ArrayList<>());
        assertThrows(IllegalArgumentException.class, () -> iterator.skipBackendResults(-1));
        iterator.hasNext();
        assertThrows(IllegalStateException.class, () -> iterator.skipBackendResults(1));
        iterator.close();
    }

    //A joint query streams its first index without a limit, keeping the ids which the other indexes agreed on, here 1,
    //3 and 4 of 1 to 4. Each read under a larger limit skips as many of the first index's ids as the reads before went
    //through, before it checks any against the others, and turns each id into an element once: read under a limit of
    //1, then 2, then 4, it returns 1, 3 and 4
    @Test
    public void shouldReadOnPastTheIdsOfTheFirstIndexOfAJointQueryWhichTheReadsBeforeWentThrough() {
        final JointIndexQuery.Subquery subQuery = mock(JointIndexQuery.Subquery.class);
        when(subQuery.getProfiler()).thenReturn(QueryProfiler.NO_OP);
        when(subQuery.hasLimit()).thenReturn(false);
        when(subQuery.getLimit()).thenReturn(Query.NO_LIMIT);
        final IndexSerializer indexSerializer = mock(IndexSerializer.class);
        when(indexSerializer.query(any(), any(), any())).thenAnswer(invocation -> Stream.of(1L, 2L, 3L, 4L));
        //A cache which collects the ids, so that closing an iterator may probe it
        final SubqueryCache indexCache = emptyCache();
        final List<Object> converted = new ArrayList<>();
        final LimitAdjustingIterator<JanusGraphElement> iterator =
            new LimitAdjustingIterator<JanusGraphElement>(Integer.MAX_VALUE - 1, 1) {
                @Override
                public Iterator<JanusGraphElement> getNewIterator(int newLimit) {
                    return new SubqueryIterator(subQuery, indexSerializer, mock(BackendTransaction.class),
                        mock(StandardJanusGraphTx.class), indexCache, newLimit, id -> {
                            converted.add(id);
                            return elementOf(id);
                        }, Arrays.asList(1L, 3L, 4L));
                }
            };
        final List<Object> returned = new ArrayList<>();
        iterator.forEachRemaining(element -> returned.add(element.id()));
        assertEquals(Arrays.asList(1L, 3L, 4L), returned);
        assertEquals(Arrays.asList(1L, 3L, 4L), converted);
    }

    private static JanusGraphElement elementOf(Object id) {
        final JanusGraphElement element = mock(JanusGraphElement.class);
        when(element.id()).thenReturn(id);
        return element;
    }

    //An index provider may build its stream by flatMapping pages, which pushes a whole page through the stream at
    //once. The ids are taken from it one at a time all the same, so that the count of ids read stops at the last
    //element found, rather than at the end of its page, and a read under a larger limit skips no id it didn't return
    @Test
    public void shouldTakeTheIdsOfAPagedIndexOneAtATime() {
        final JointIndexQuery.Subquery subQuery = mock(JointIndexQuery.Subquery.class);
        when(subQuery.getProfiler()).thenReturn(QueryProfiler.NO_OP);
        when(subQuery.hasLimit()).thenReturn(false);
        when(subQuery.getLimit()).thenReturn(Query.NO_LIMIT);
        final List<List<Object>> pages = Arrays.asList(Arrays.<Object>asList(1L, 2L, 3L, 4L),
            Arrays.<Object>asList(5L, 6L, 7L, 8L), Arrays.<Object>asList(9L, 10L, 11L, 12L));
        final List<Object> ids = pages.stream().flatMap(List::stream).collect(Collectors.toList());
        final IndexSerializer indexSerializer = mock(IndexSerializer.class);
        when(indexSerializer.query(any(), any(), any())).thenAnswer(invocation -> pages.stream().flatMap(List::stream));
        //The first index of a joint query, which the others agree on whole
        final LimitAdjustingIterator<JanusGraphElement> iterator =
            new LimitAdjustingIterator<JanusGraphElement>(Integer.MAX_VALUE - 1, 3) {
                @Override
                public Iterator<JanusGraphElement> getNewIterator(int newLimit) {
                    return new SubqueryIterator(subQuery, indexSerializer, mock(BackendTransaction.class),
                        mock(StandardJanusGraphTx.class), emptyCache(), newLimit, SubqueryIteratorTest::elementOf, ids);
                }
            };
        final List<Object> returned = new ArrayList<>();
        iterator.forEachRemaining(element -> returned.add(element.id()));
        assertEquals(ids, returned);
    }

    //Closing the iterator before its end probes for its end, which reads on, but the count of ids read stays at the
    //last element found
    @Test
    public void shouldKeepTheCountOfIdsReadAtTheLastElementFoundWhereClosingReadsOn() {
        final List<Object> converted = new ArrayList<>();
        final SubqueryIterator iterator = skippingIterator(subqueryLimitedToFive(), emptyCache(), converted);
        iterator.next();
        iterator.next();
        iterator.close();
        assertEquals(Arrays.asList(1L, 2L, 3L), converted);
        assertEquals(2, iterator.getBackendResultsRead());
        assertFalse(iterator.hasBackendReachedLimit());
    }

    //An iterator which found as many elements as its read limit allows takes no further id from the index, not even
    //to tell whether one follows, where a consumer stops at the limit and closes it: a stream which reads the index in
    //pages could read a page for it
    @Test
    public void shouldTakeNoIdFromTheIndexPastTheReadLimit() {
        final AtomicInteger taken = new AtomicInteger();
        final IndexSerializer indexSerializer = mock(IndexSerializer.class);
        when(indexSerializer.query(any(), any(), any()))
            .thenAnswer(invocation -> Stream.of(1L, 2L, 3L, 4L, 5L).peek(id -> taken.incrementAndGet()));
        final SubqueryIterator iterator = new SubqueryIterator(subqueryLimitedToFive(), indexSerializer,
            mock(BackendTransaction.class), mock(StandardJanusGraphTx.class), emptyCache(), 2,
            id -> mock(JanusGraphElement.class), null);
        iterator.next();
        iterator.next();
        iterator.close();
        assertEquals(2, taken.get());
    }

    //The probe of closing looks ahead as hasNext does: a read after it returns the element which the probe found,
    //without turning its id into an element again
    @Test
    public void shouldReturnTheElementWhichTheProbeOfClosingFoundToTheNextRead() {
        final IndexSerializer indexSerializer = mock(IndexSerializer.class);
        when(indexSerializer.query(any(), any(), any())).thenAnswer(invocation -> Stream.of(1L, 2L, 3L, 4L, 5L));
        final List<Object> converted = new ArrayList<>();
        final SubqueryIterator iterator = new SubqueryIterator(subqueryLimitedToFive(), indexSerializer,
            mock(BackendTransaction.class), mock(StandardJanusGraphTx.class), emptyCache(), 5, id -> {
                converted.add(id);
                return elementOf(id);
            }, null);
        iterator.next();
        iterator.close();
        assertEquals(Arrays.asList(1L, 2L), converted);
        assertEquals(2L, iterator.next().id());
        assertEquals(Arrays.asList(1L, 2L), converted);
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
