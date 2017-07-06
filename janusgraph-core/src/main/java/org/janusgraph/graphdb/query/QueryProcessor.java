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
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.janusgraph.core.QueryException;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.graphdb.query.profile.QueryProfiler;

import javax.annotation.Nullable;
import java.util.*;

/**
 * Executes a given {@link ElementQuery} against a provided {@link QueryExecutor} to produce the result set of elements.
 * </p>
 * The QueryProcessor creates a number of stacked iterators. </br>
 * At the highest level, the OuterIterator ensures that the correct (up to the given limit) number of elements is returned. It also provides the implementation of remove()
 * by calling the element's remove() method. </br>
 * The OuterIterator wraps the "unfolded" iterator which is a combination of the individual result set iterators of the sub-queries of the given query (see {@link ElementQuery#getSubQuery(int)}.
 * The unfolded iterator combines this iterators by checking whether 1) the result sets need additional filtering (if so, a filter iterator is wrapped around it) and 2) whether
 * the final result set needs to be sorted and in what order. If the result set needs to be sorted and the individual sub-query result sets aren't, then a PreSortingIterator is wrapped around
 * the iterator which effectively iterates the result set out, sorts it and then returns an iterator (i.e. much more expensive than exploiting existing sort orders).</br>
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
    public Iterator<R> iterator() {
        if (query.isEmpty())
            return Collections.emptyIterator();

        return new ResultSetIterator(getUnfoldedIterator(),(query.hasLimit()) ? query.getLimit() : Query.NO_LIMIT);
    }

    private Iterator<R> getUnfoldedIterator() {
        Iterator<R> iter = null;
        boolean hasDeletions = executor.hasDeletions(query);
        Iterator<R> newElements = executor.getNew(query);
        if (query.isSorted()) {
            for (int i = query.numSubQueries() - 1; i >= 0; i--) {
                BackendQueryHolder<B> subq = query.getSubQuery(i);
                Iterator<R> subqiter = getFilterIterator((subq.isSorted())
                                                            ? new LimitAdjustingIterator(subq)
                                                            : new PreSortingIterator(subq),
                                                         hasDeletions,
                                                         !subq.isFitted());

                iter = (iter == null)
                        ? subqiter
                        : new ResultMergeSortIterator<R>(subqiter, iter, query.getSortOrder(), query.hasDuplicateResults());
            }

            Preconditions.checkArgument(iter != null);

            if (newElements.hasNext()) {
                final List<R> allNew = Lists.newArrayList(newElements);
                Collections.sort(allNew, query.getSortOrder());
                iter = new ResultMergeSortIterator<R>(allNew.iterator(), iter, query.getSortOrder(), query.hasDuplicateResults());
            }
        } else {
            final Set<R> allNew;
            if (newElements.hasNext()) {
                allNew = Sets.newHashSet(newElements);
            } else {
                allNew = ImmutableSet.of();
            }

            List<Iterator<R>> iters = new ArrayList<Iterator<R>>(query.numSubQueries());
            for (int i = 0; i < query.numSubQueries(); i++) {
                BackendQueryHolder<B> subq = query.getSubQuery(i);
                Iterator<R> subiter = new LimitAdjustingIterator(subq);
                subiter = getFilterIterator(subiter, hasDeletions, !subq.isFitted());
                if (!allNew.isEmpty()) {
                    subiter = Iterators.filter(subiter, new Predicate<R>() {
                        @Override
                        public boolean apply(@Nullable R r) {
                            return !allNew.contains(r);
                        }
                    });
                }
                iters.add(subiter);
            }
            if (iters.size() > 1) {
                iter = Iterators.concat(iters.iterator());
                if (query.hasDuplicateResults()) { //Cache results and filter out duplicates
                    final Set<R> seenResults = new HashSet<R>();
                    iter = Iterators.filter(iter, new Predicate<R>() {
                        @Override
                        public boolean apply(@Nullable R r) {
                            if (seenResults.contains(r)) return false;
                            else {
                                seenResults.add(r);
                                return true;
                            }
                        }
                    });
                }
            } else iter = iters.get(0);

            if (!allNew.isEmpty()) iter = Iterators.concat(allNew.iterator(), iter);
        }
        return iter;
    }

    private Iterator<R> getFilterIterator(final Iterator<R> iter, final boolean filterDeletions, final boolean filterMatches) {
        if (filterDeletions || filterMatches) {
            return Iterators.filter(iter, new Predicate<R>() {
                @Override
                public boolean apply(@Nullable R r) {
                    return (!filterDeletions || !executor.isDeleted(query, r)) && (!filterMatches || query.matches(r));
                }
            });
        } else {
            return iter;
        }
    }

    private final class PreSortingIterator implements Iterator<R> {

        private final Iterator<R> iter;

        private PreSortingIterator(BackendQueryHolder<B> backendQueryHolder) {
            List<R> all = Lists.newArrayList(executor.execute(query,
                    backendQueryHolder.getBackendQuery().updateLimit(MAX_SORT_ITERATION),
                    backendQueryHolder.getExecutionInfo(),backendQueryHolder.getProfiler()));
            if (all.size() >= MAX_SORT_ITERATION)
                throw new QueryException("Could not execute query since pre-sorting requires fetching more than " +
                        MAX_SORT_ITERATION + " elements. Consider rewriting the query to exploit sort orders");
            Collections.sort(all, query.getSortOrder());
            iter = all.iterator();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public R next() {
            return iter.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

     /*
     TODO: Make the returned iterator smarter about limits: If less than LIMIT elements are returned,
     it checks if the underlying iterators have been exhausted. If not, then it doubles the limit, discards the first count
     elements and returns the remaining ones. Tricky bit: how to keep track of which iterators have been exhausted?
     */


    private final class LimitAdjustingIterator extends org.janusgraph.graphdb.query.LimitAdjustingIterator<R> {

        private B backendQuery;
        private QueryProfiler profiler;
        private final Object executionInfo;

        private LimitAdjustingIterator(BackendQueryHolder<B> backendQueryHolder) {
            super(Integer.MAX_VALUE-1,backendQueryHolder.getBackendQuery().getLimit());
            this.backendQuery = backendQueryHolder.getBackendQuery();
            this.executionInfo = backendQueryHolder.getExecutionInfo();
            this.profiler = backendQueryHolder.getProfiler();
        }

        @Override
        public Iterator<R> getNewIterator(int newLimit) {
            if (!backendQuery.hasLimit() || newLimit>backendQuery.getLimit())
                backendQuery = backendQuery.updateLimit(newLimit);
            return executor.execute(query, backendQuery, executionInfo, profiler);
        }

    }




}
