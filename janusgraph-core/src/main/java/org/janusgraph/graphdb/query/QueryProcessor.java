// Copyright 2017 JanusGraph Authors
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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.QueryException;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.graph.GraphCentricQuery;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.util.CloseableIteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Executes a given {@link ElementQuery} against a provided {@link QueryExecutor} to produce the result set of elements.
 * <p>
 * The QueryProcessor creates a number of stacked iterators. <br>
 * At the highest level, the OuterIterator ensures that the correct (up to the given limit) number of elements is returned. It also provides the implementation of remove()
 * by calling the element's remove() method. <br>
 * The OuterIterator wraps the "unfolded" iterator which is a combination of the individual result set iterators of the sub-queries of the given query (see {@link ElementQuery#getSubQuery(int)}.
 * The unfolded iterator combines this iterators by checking whether 1) the result sets need additional filtering (if so, a filter iterator is wrapped around it) and 2) whether
 * the final result set needs to be sorted and in what order. If the result set needs to be sorted and the individual sub-query result sets aren't, then a PreSortingIterator is wrapped around
 * the iterator which effectively iterates the result set out, sorts it and then returns an iterator (i.e. much more expensive than exploiting existing sort orders).<br>
 * In this way, the individual sub-result sets are prepared and then merged together the MergeSortIterator (which conserves sort order if present).
 * The semantics of the queries is OR, meaning the result sets are combined.
 * However, when {@link org.janusgraph.graphdb.query.ElementQuery#hasDuplicateResults()} is true (which assumes that the result set is sorted) then the merge sort iterator
 * filters out immediate duplicates.
 *
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class QueryProcessor<Q extends ElementQuery<R, B>, R extends JanusGraphElement, B extends BackendQuery<B>> implements Iterable<R> {

    private static final int MAX_SORT_ITERATION = 1000000;

    private final Q query;
    private final QueryExecutor<Q, R, B> executor;

    public QueryProcessor(Q query, QueryExecutor<Q, R, B> executor) {
        Preconditions.checkNotNull(query);
        Preconditions.checkNotNull(executor);
        this.query = query;
        this.executor = executor;
    }

    @Override
    public CloseableIterator<R> iterator() {
        if (query.isEmpty())
            return CloseableIteratorUtils.emptyIterator();

        return new ResultSetIterator(getUnfoldedIterator(),(query.hasLimit()) ? query.getLimit() : Query.NO_LIMIT);
    }

    private Iterator<R> getUnfoldedIterator() {
        Iterator<R> iterator = null;
        boolean hasDeletions = executor.hasDeletions(query);
        Iterator<R> newElements = executor.getNew(query);
        if (query.isSorted()) {
            //An element which this transaction changed may come from the backend of a graph query as well as among
            //the new elements, so the backend's are left out where they are among those, as below for an unsorted
            //query. A vertex-centric query's backend returns the relations stored before the transaction, none of which
            //is new, and of which one the transaction replaced is deleted
            final List<R> allNew = new ArrayList<>();
            newElements.forEachRemaining(allNew::add);
            final Set<R> allNewSet = allNew.isEmpty() || !(query instanceof GraphCentricQuery)
                ? Collections.emptySet() : new HashSet<>(allNew);
            final List<R> sortedNew = allNew.isEmpty() ? allNew : sorted(allNew);
            //Where the new elements fill the limit, no result whose value of the first order key comes after that of
            //the last of them within the limit can come within it either, so a backend which sorts may leave those out
            final Object lastValue = lastFirstOrderValueWithinLimit(sortedNew);
            for (int i = query.numSubQueries() - 1; i >= 0; i--) {
                BackendQueryHolder<B> subquery = query.getSubQuery(i);
                if (lastValue != null && subquery.isSorted()) {
                    final B narrowed = executor.withoutResultsAfter(query, subquery.getBackendQuery(), lastValue);
                    if (narrowed != subquery.getBackendQuery()) {
                        subquery = subquery.withBackendQuery(narrowed);
                    }
                }
                CloseableIterator<R> subqueryIterator = subquery.isSorted()
                    ? new LimitAdjustingIterator(subquery) : new PreSortingIterator(subquery);
                if (!allNewSet.isEmpty()) {
                    //Left out before they are checked, as the new elements stand for them
                    subqueryIterator = CloseableIteratorUtils.filter(subqueryIterator, r -> !allNewSet.contains(r));
                }
                subqueryIterator = getFilterIterator(subqueryIterator, hasDeletions, !subquery.isFitted());

                iterator = (iterator == null)
                        ? subqueryIterator
                        : new ResultMergeSortIterator<>(subqueryIterator, iterator, query.getSortOrder(), query.hasDuplicateResults());
            }

            Preconditions.checkArgument(iterator != null);

            if (!allNew.isEmpty()) {
                iterator = new ResultMergeSortIterator<>(sortedNew.iterator(), iterator,
                    query.getSortOrder(), query.hasDuplicateResults());
            }
        } else {
            //The new elements come first, each checked against the query only once it is reached, so that a limit which
            //they fill checks no more of them, and doesn't read the backend; the backend's results come after the last
            //of them, by which time allNew holds every new element, so that those among the backend's results are left
            //out
            final boolean hasNew = newElements.hasNext();
            final Set<R> allNew = hasNew ? new HashSet<>() : Collections.emptySet();

            final List<Iterator<R>> iterators = new ArrayList<>(query.numSubQueries());
            for (int i = 0; i < query.numSubQueries(); i++) {
                final BackendQueryHolder<B> subquery = query.getSubQuery(i);
                CloseableIterator<R> subIterator = new LimitAdjustingIterator(subquery);
                if (hasNew) {
                    //Left out before they are checked, as for an ordered query
                    subIterator = CloseableIteratorUtils.filter(subIterator, r -> !allNew.contains(r));
                }
                subIterator = getFilterIterator(subIterator, hasDeletions, !subquery.isFitted());
                iterators.add(subIterator);
            }
            if (iterators.size() > 1) {
                iterator = CloseableIteratorUtils.concat(iterators);
                if (query.hasDuplicateResults()) { //Cache results and filter out duplicates
                    final Set<R> seenResults = new HashSet<>();
                    iterator = CloseableIteratorUtils.filter(iterator, r -> {
                        if (seenResults.contains(r)) return false;
                        else {
                            seenResults.add(r);
                            return true;
                        }
                    });
                }
            } else iterator = iterators.get(0);

            if (hasNew) {
                final Iterator<R> collected = Iterators.transform(newElements, r -> {
                    allNew.add(r);
                    return r;
                });
                iterator = CloseableIteratorUtils.concat(collected, iterator);
            }
        }
        return iterator;
    }

    /*
     * The value by which the first order key orders the last new element within the query's limit, where the new
     * elements fill it, so that no element whose value comes after it in the query's order can come within the limit.
     * Null where they don't fill it, where the query may return an element twice, where its order isn't one of order
     * keys, or where that element has no value of the key, as a missing value comes last.
     */
    private Object lastFirstOrderValueWithinLimit(List<R> sortedNew) {
        if (!query.hasLimit() || query.hasDuplicateResults() || sortedNew.size() < query.getLimit()
                || !(query.getSortOrder() instanceof OrderList)) {
            return null;
        }
        return ((OrderList) query.getSortOrder()).value(sortedNew.get(query.getLimit() - 1), 0);
    }

    /*
     * The new elements in the order of the query. Those of a graph query are compared by the values of its order keys,
     * read once for each element instead of twice for each comparison, and elements of equal values keep the order in
     * which the transaction gave them, as a stable sort of all of them does, so that the first of them under a limit
     * are the first under a larger limit as well. Where the query has a limit, and the merge drops no duplicates, which
     * it does for no graph query, only as many of them are kept as the limit takes, the first, as no later one can come
     * before its end; otherwise a transaction which changed the order key of many vertices would have them all sorted
     * on each such query.
     */
    private List<R> sorted(List<R> allNew) {
        final Comparator<R> sortOrder = query.getSortOrder();
        if (!(sortOrder instanceof OrderList) || allNew.size() < 2) {
            allNew.sort(sortOrder);
            return allNew;
        }
        final OrderList orders = (OrderList) sortOrder;
        final Comparator<Ranked<R>> byValues = (r1, r2) -> {
            final int cmp = orders.compareValues(r1.values, r2.values);
            return cmp != 0 ? cmp : Integer.compare(r1.position, r2.position);
        };
        final int kept = query.hasLimit() && !query.hasDuplicateResults()
            ? Math.min(query.getLimit(), allNew.size()) : allNew.size();
        final List<Ranked<R>> first;
        if (kept < allNew.size()) {
            //A heap of the first elements so far, whose head is the last of them: each element is added, and the head
            //removed once there are more than are kept
            final PriorityQueue<Ranked<R>> heap = new PriorityQueue<>(kept + 1, byValues.reversed());
            for (int i = 0; i < allNew.size(); i++) {
                heap.add(new Ranked<>(allNew.get(i), orders.values(allNew.get(i)), i));
                if (heap.size() > kept) {
                    heap.poll();
                }
            }
            first = new ArrayList<>(heap);
        } else {
            first = new ArrayList<>(allNew.size());
            for (int i = 0; i < allNew.size(); i++) {
                first.add(new Ranked<>(allNew.get(i), orders.values(allNew.get(i)), i));
            }
        }
        first.sort(byValues);
        final List<R> sorted = new ArrayList<>(first.size());
        for (final Ranked<R> ranked : first) {
            sorted.add(ranked.element);
        }
        return sorted;
    }

    //A new element with the values of the query's order keys and its place among the new elements
    private static final class Ranked<R> {
        private final R element;
        private final Object[] values;
        private final int position;

        private Ranked(R element, Object[] values, int position) {
            this.element = element;
            this.values = values;
            this.position = position;
        }
    }

    private CloseableIterator<R> getFilterIterator(final CloseableIterator<R> iterator, final boolean filterDeletions, final boolean filterMatches) {
        if (filterDeletions || filterMatches) {
            return CloseableIteratorUtils.filter(iterator, r -> (!filterDeletions || !executor.isDeleted(query, r)) && (!filterMatches || query.matches(r)));
        } else {
            return iterator;
        }
    }

    private final class PreSortingIterator implements CloseableIterator<R> {

        private final Iterator<R> iterator;

        /*
         * Sorts what the backend returns, once it has left out the elements which the transaction removed, as the
         * order values of a removed vertex can't be read. getFilterIterator leaves out the other elements which the
         * transaction deleted, such as those which it changed so that they no longer match, as they are reached, as
         * for an unsorted query, so that a change which the caller makes while it reads the results is seen.
         */
        private PreSortingIterator(BackendQueryHolder<B> backendQueryHolder) {
            final List<R> all = new ArrayList<>();
            final Iterator<R> results = executor.execute(query,
                backendQueryHolder.getBackendQuery().updateLimit(MAX_SORT_ITERATION),
                backendQueryHolder.getExecutionInfo(),backendQueryHolder.getProfiler());
            int fetched = 0;
            while (results.hasNext()) {
                final R result = results.next();
                fetched++;
                if (!result.isRemoved()) {
                    all.add(result);
                }
            }
            if (fetched >= MAX_SORT_ITERATION)
                throw new QueryException("Could not execute query since pre-sorting requires fetching more than " +
                        MAX_SORT_ITERATION + " elements. Consider rewriting the query to exploit sort orders");
            all.sort(query.getSortOrder());
            iterator = all.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public R next() {
            return iterator.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            CloseableIterator.closeIterator(iterator);
        }
    }

    private final class LimitAdjustingIterator extends org.janusgraph.graphdb.query.LimitAdjustingIterator<R> {

        private B backendQuery;
        private final QueryProfiler profiler;
        private final Object executionInfo;

        private LimitAdjustingIterator(BackendQueryHolder<B> backendQueryHolder) {
            super(Integer.MAX_VALUE-1,backendQueryHolder.getBackendQuery().getLimit());
            this.backendQuery = backendQueryHolder.getBackendQuery();
            this.executionInfo = backendQueryHolder.getExecutionInfo();
            this.profiler = backendQueryHolder.getProfiler();
        }

        @Override
        public Iterator<R> getNewIterator(int newLimit) {
            if (newLimit>backendQuery.getLimit())
                backendQuery = backendQuery.updateLimit(newLimit);
            return executor.execute(query, backendQuery, executionInfo, profiler);
        }

    }

}
