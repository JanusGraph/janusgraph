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

package org.janusgraph.graphdb.query;

import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * How {@link QueryProcessor} combines the transaction's new elements with what the backend returns: an unordered query
 * takes the new elements only as far as its limit needs them, and an ordered one, whose new elements fill its limit,
 * asks the backend only for the results up to the last of them within the limit.
 */
public class QueryProcessorTest {

    private final PropertyKey key = mock(PropertyKey.class);

    @Test
    public void takesTheNewElementsOfAnUnorderedQueryOnlyAsFarAsItsLimit() {
        final List<JanusGraphElement> news = elements(1L, 2L, 3L, 4L, 5L, 6L);
        final Executor executor = new Executor(news, elements(7L, 8L));
        final List<JanusGraphElement> result = run(query(false, 3, true), executor);
        assertEquals(news.subList(0, 3), result);
        assertEquals(3, executor.newTaken);
        assertEquals(0, executor.executed.size());
    }

    @Test
    public void readsTheBackendOfAnUnorderedQueryOnceItsNewElementsRunOut() {
        final List<JanusGraphElement> news = elements(1L, 2L);
        final List<JanusGraphElement> stored = elements(3L, 4L, 5L);
        final List<JanusGraphElement> backend = new ArrayList<>(Collections.singletonList(news.get(1)));
        backend.addAll(stored);
        final Executor executor = new Executor(news, backend);
        final List<JanusGraphElement> result = run(query(false, 4, true), executor);
        assertEquals(Arrays.asList(news.get(0), news.get(1), stored.get(0), stored.get(1)), result);
        assertEquals(2, executor.newTaken);
        assertTrue(executor.executed.size() > 0);
    }

    @Test
    public void asksTheBackendOfAnOrderedQueryForTheResultsUpToItsLastNewValueWithinTheLimit() {
        final List<JanusGraphElement> news = elements(5L, 9L, 7L);
        final List<JanusGraphElement> stored = elements(8L, 6L, 4L);
        final Executor executor = new Executor(news, stored);
        executor.narrows = true;
        final List<JanusGraphElement> result = run(query(true, 2, true), executor);
        assertEquals(Arrays.asList(news.get(1), stored.get(0)), result);
        assertEquals(7L, executor.lastValue);
        assertTrue(executor.executed.stream().allMatch(backendQuery -> backendQuery.narrowed));
    }

    @Test
    public void asksNothingWhereTheNewElementsDontFillTheLimit() {
        final Executor executor = new Executor(elements(5L, 9L, 7L), elements(8L, 6L));
        executor.narrows = true;
        assertEquals(4, run(query(true, 4, true), executor).size());
        assertNull(executor.lastValue);
        assertTrue(executor.executed.stream().noneMatch(backendQuery -> backendQuery.narrowed));
    }

    @Test
    public void asksNothingWhereTheLastNewElementWithinTheLimitHasNoValue() {
        final Executor executor = new Executor(elements(9L, null), elements(8L, 6L));
        executor.narrows = true;
        assertEquals(2, run(query(true, 2, true), executor).size());
        assertNull(executor.lastValue);
    }

    @Test
    public void asksNothingOfABackendWhichDoesntSort() {
        final Executor executor = new Executor(elements(5L, 9L, 7L), elements(8L, 6L));
        executor.narrows = true;
        assertEquals(2, run(query(true, 2, false), executor).size());
        assertNull(executor.lastValue);
    }

    private List<JanusGraphElement> run(ElementQuery<JanusGraphElement, FakeBackendQuery> query, Executor executor) {
        final List<JanusGraphElement> result = new ArrayList<>();
        new QueryProcessor<>(query, executor).iterator().forEachRemaining(result::add);
        return result;
    }

    //Elements whose values of the key are the given ones, null for none
    private List<JanusGraphElement> elements(Long... values) {
        final List<JanusGraphElement> elements = new ArrayList<>();
        for (final Long value : values) {
            final JanusGraphElement element = mock(JanusGraphElement.class);
            doReturn(value).when(element).valueOrNull(key);
            elements.add(element);
        }
        return elements;
    }

    //A query of a limit, unordered or in the descending order of the key, whose backend sorts or doesn't
    @SuppressWarnings("unchecked")
    private ElementQuery<JanusGraphElement, FakeBackendQuery> query(boolean ordered, int limit, boolean backendSorts) {
        final ElementQuery<JanusGraphElement, FakeBackendQuery> query = mock(ElementQuery.class);
        final OrderList orders = new OrderList();
        orders.add(key, Order.DESC);
        when(query.isEmpty()).thenReturn(false);
        when(query.numSubQueries()).thenReturn(1);
        when(query.getSubQuery(0)).thenReturn(
            new BackendQueryHolder<>(new FakeBackendQuery(false, limit), true, backendSorts));
        when(query.isSorted()).thenReturn(ordered);
        when(query.getSortOrder()).thenReturn((Comparator) orders);
        when(query.hasDuplicateResults()).thenReturn(false);
        when(query.hasLimit()).thenReturn(true);
        when(query.getLimit()).thenReturn(limit);
        when(query.matches(any())).thenReturn(true);
        return query;
    }

    private static final class FakeBackendQuery extends BaseQuery implements BackendQuery<FakeBackendQuery> {
        private final boolean narrowed;

        private FakeBackendQuery(boolean narrowed, int limit) {
            super(limit);
            this.narrowed = narrowed;
        }

        @Override
        public FakeBackendQuery updateLimit(int newLimit) {
            return new FakeBackendQuery(narrowed, newLimit);
        }
    }

    //Gives the new elements, counting how many were taken, and the backend's results in their order up to the limit of
    //each execution, which it records; narrows a query, if it does, by marking it
    private static final class Executor implements
            QueryExecutor<ElementQuery<JanusGraphElement, FakeBackendQuery>, JanusGraphElement, FakeBackendQuery> {
        private final List<JanusGraphElement> news;
        private final List<JanusGraphElement> backend;
        private final List<FakeBackendQuery> executed = new ArrayList<>();
        private int newTaken;
        private boolean narrows;
        private Object lastValue;

        private Executor(List<JanusGraphElement> news, List<JanusGraphElement> backend) {
            this.news = news;
            this.backend = backend;
        }

        @Override
        public Iterator<JanusGraphElement> getNew(ElementQuery<JanusGraphElement, FakeBackendQuery> query) {
            final Iterator<JanusGraphElement> elements = news.iterator();
            return new Iterator<JanusGraphElement>() {
                @Override
                public boolean hasNext() {
                    return elements.hasNext();
                }

                @Override
                public JanusGraphElement next() {
                    newTaken++;
                    return elements.next();
                }
            };
        }

        @Override
        public boolean hasDeletions(ElementQuery<JanusGraphElement, FakeBackendQuery> query) {
            return false;
        }

        @Override
        public boolean isDeleted(ElementQuery<JanusGraphElement, FakeBackendQuery> query, JanusGraphElement result) {
            return false;
        }

        @Override
        public Iterator<JanusGraphElement> execute(ElementQuery<JanusGraphElement, FakeBackendQuery> query,
                                                   FakeBackendQuery subquery, Object executionInfo,
                                                   QueryProfiler profiler) {
            executed.add(subquery);
            return backend.subList(0, Math.min(subquery.getLimit(), backend.size())).iterator();
        }

        @Override
        public FakeBackendQuery withoutResultsAfter(ElementQuery<JanusGraphElement, FakeBackendQuery> query,
                                                    FakeBackendQuery subquery, Object value) {
            lastValue = value;
            return narrows ? new FakeBackendQuery(true, subquery.getLimit()) : subquery;
        }
    }
}
