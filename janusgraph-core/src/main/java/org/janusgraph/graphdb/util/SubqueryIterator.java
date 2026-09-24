/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.janusgraph.graphdb.util;

import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.transaction.subquerycache.SubqueryCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * @author davidclement90@laposte.net
 */
public class SubqueryIterator extends CloseableAbstractIterator<JanusGraphElement> {
    private static final Logger log = LoggerFactory.getLogger(SubqueryIterator.class);

    private final JointIndexQuery.Subquery subQuery;

    private final SubqueryCache indexCache;

    private Iterator<? extends JanusGraphElement> elementIterator;

    private List<Object> currentIds;

    private QueryProfiler profiler;

    private boolean isTimerRunning;

    //The limit the index is read under, which a joint query takes from itself. Distinct from subQuery.getLimit(),
    //which is the limit the cache records the result against
    private final int readLimit;

    private int emittedCount;

    //Set when computeNext saw the stream end. Not the whole story: a caller which stops at the limit never reaches
    //that point, so close falls back to asking the iterator
    private boolean exhausted;

    public SubqueryIterator(JointIndexQuery.Subquery subQuery, IndexSerializer indexSerializer,
                            BackendTransaction backendTx,
                            StandardJanusGraphTx tx,
                            SubqueryCache indexCache, int readLimit,
                            Function<Object, ? extends JanusGraphElement> function, List<Object> otherResults) {
        this.subQuery = subQuery;
        this.indexCache = indexCache;
        this.readLimit = readLimit;
        final List<Object> cacheResponse = indexCache.getIfPresent(subQuery);
        final Stream<?> stream;
        if (cacheResponse != null) {
            stream = cacheResponse.stream();
        } else {
            try {
                currentIds = new ArrayList<>();
                profiler = QueryProfiler.startProfile(subQuery.getProfiler(), subQuery);
                isTimerRunning = true;
                stream = indexSerializer.query(subQuery, backendTx, tx).peek(r -> currentIds.add(r));
            } catch (final Exception e) {
                throw new JanusGraphException("Could not call index", e);
            }
        }
        //Membership is tested once for every element the first index returns, and otherResults is deliberately
        //unbounded: StandardJanusGraphTx passes NO_LIMIT to processIntersectingRetrievals so that the intersection is
        //complete. Scanning the list for each element would make the intersection cost O(n*m).
        //Hashing the ids matches how the intersection is already computed: processIntersectingRetrievals narrows
        //its result with a HashSet of each further index's results, so membership among these ids is hash-based
        //there too
        final Set<Object> otherResultSet = otherResults == null ? null : new HashSet<>(otherResults);
        elementIterator = stream
                .filter(e -> otherResultSet == null || otherResultSet.contains(e))
                .map(e -> {
                    JanusGraphElement r = function.apply(e);
                    if (r == null) {
                        log.warn("Subquery returned invalid element id: {}", e);
                    }
                    return r;
                })
                .filter(r -> r != null) // ignore invalid elements
                .limit(readLimit)
                .iterator();
    }

    @Override
    protected JanusGraphElement computeNext() {
        if (elementIterator.hasNext()) {
            //Counted here rather than in the stream, because the count decides whether the result is safe to cache
            //and so is part of the contract rather than an observation of it. Counted once the element is in hand,
            //so that a next() which threw cannot leave the count reporting an element that was never produced
            final JanusGraphElement next = elementIterator.next();
            emittedCount++;
            return next;
        }
        exhausted = true;
        close();
        return endOfData();
    }

    /**
     * Close the iterator, stop timer and update profiler.
     * Put results into cache only if no later query can ask for more results than the cached list holds.
     */
    @Override
    public void close() {
        if (isTimerRunning) {
            assert currentIds != null;
            //computeNext records the stream ending, which spares the probe below on the common path. The probe is
            //still needed when it did not: a caller which took exactly as many elements as the limit allowed stops
            //without ever driving the iterator to its end, and that result is complete and worth caching. Asking
            //hasNext is the only way to tell that apart from a caller which stopped early by choice
            if ((exhausted || !elementIterator.hasNext()) && isSafeToCache()) {
                indexCache.put(subQuery, currentIds);
            }
            profiler.setResultSize(currentIds.size());
            profiler.stopTimer();
            isTimerRunning = false;
        }
    }

    /**
     * Whether the collected ids can be stored without a later query being served fewer results than it asked for.
     * <p>
     * This assumes the contract of
     * {@link org.janusgraph.graphdb.transaction.subquerycache.SubsetSubqueryCache}, which every caching
     * {@link org.janusgraph.graphdb.transaction.subquerycache.SubqueryCache} in the tree extends: a result is
     * recorded against {@code subQuery.getLimit()} and served back when a later query asks for no more than that,
     * or when the recorded limit exceeds the size of the list and the list is therefore known to be complete. An
     * implementation which keyed results differently would need this method revisited. So the ids are
     * safe to store in either of two cases: fewer elements were emitted than {@code readLimit}, meaning the read
     * limit never stopped the index and the list holds every result; or the subquery carries a limit no wider than
     * the one the list was read under, which is what keeps a truncated prefix from being served to a query that
     * asked for more.
     * <p>
     * The second case is what {@link org.janusgraph.graphdb.query.graph.JointIndexQuery#updateLimit(int)} gives a
     * single subquery by propagating the limit into it, and withholds once a joint query has more than one, which
     * is the divergence reported in issue #4931.
     */
    private boolean isSafeToCache() {
        return emittedCount < readLimit || subQuery.getLimit() <= readLimit;
    }

}
