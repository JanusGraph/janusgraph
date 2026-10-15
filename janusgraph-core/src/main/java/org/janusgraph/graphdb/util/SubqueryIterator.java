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

import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.query.BackendLimitAware;
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
public class SubqueryIterator extends CloseableAbstractIterator<JanusGraphElement> implements BackendLimitAware {
    private static final Logger log = LoggerFactory.getLogger(SubqueryIterator.class);

    private final JointIndexQuery.Subquery subQuery;

    private final SubqueryCache indexCache;

    //The ids, from the index or from the cache, counted as they are taken from here, one at a time, however the index
    //builds its stream: a stream which flatMaps pages pushes a whole page through its steps at once, so a step added to
    //it would count the ids of the page past the element found. findNext skips, checks and converts them itself, as a
    //stream built on this iterator would cost every id a second pipeline
    private final Iterator<?> ids;

    //Set once the iterator looked for its first element, after which skipBackendResults can't leave out ids any more
    private boolean started;

    //The results the other indexes of a joint query agreed on, which an id has to be among, or null for none
    private final Set<Object> otherResultSet;

    private final Function<Object, ? extends JanusGraphElement> function;

    //How many ids the read skips before it turns any into an element, which skipBackendResults sets
    private long toSkip;

    //How many of those it skipped so far
    private long skipped;

    //Kept to be closed: an index backend may hold a resource behind its stream, such as an Elasticsearch scroll
    //context, which closing the stream hands back. Null for a result from the cache, and once closed, so that it is
    //closed once however often the iterator is
    private Stream<?> indexStream;

    //The ids read from the index, collected for the cache while it could keep them. Null once there are more than the
    //cache keeps, as the cache would drop the list, and null when the result came from the cache or the cache keeps
    //no list
    private List<Object> currentIds;

    //How many ids the cache keeps in a list, which currentIds holds no more than
    private final int maximumCachedResultSize;

    //How many ids were read, from the index or from the cache, those skipped included, for the profile of a read of
    //the index
    private long readCount;

    //How many ids were read up to the last element found, or up to the end once the iterator came to it, so that a
    //probe of close, which may read on, doesn't move it: how far into the subquery's results this iterator got, and
    //whether it read as many as the subquery's limit allows
    private long position;

    private QueryProfiler profiler;

    private boolean isTimerRunning;

    //The limit the index is read under, which a joint query takes from itself. Distinct from subQuery.getLimit(),
    //which is the limit the cache records the result against
    private final int readLimit;

    private int emittedCount;

    //The element which the probe of close found, which the next read returns
    private JanusGraphElement probed;

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
        this.maximumCachedResultSize = indexCache.maximumCachedResultSize();
        final List<Object> cacheResponse = indexCache.getIfPresent(subQuery);
        if (cacheResponse != null) {
            ids = cacheResponse.iterator();
        } else {
            try {
                currentIds = maximumCachedResultSize >= 0 ? new ArrayList<>() : null;
                profiler = QueryProfiler.startProfile(subQuery.getProfiler(), subQuery);
                isTimerRunning = true;
                indexStream = indexSerializer.query(subQuery, backendTx, tx);
                ids = indexStream.iterator();
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
        otherResultSet = otherResults == null ? null : new HashSet<>(otherResults);
        this.function = function;
    }

    //The next element past the skipped ids, or null where the ids ran out, or where the iterator found as many elements
    //as readLimit allows, without reading a further id then. The skipped ids are read, counted and collected for the
    //cache all the same. Called only while no element which the probe of close found waits, so that emittedCount is how
    //many elements were found
    private JanusGraphElement findNext() {
        started = true;
        while (emittedCount < readLimit && ids.hasNext()) {
            final Object id = ids.next();
            collect(id);
            if (skipped < toSkip) {
                skipped++;
            } else if (otherResultSet == null || otherResultSet.contains(id)) {
                final JanusGraphElement element = function.apply(id);
                if (element != null) {
                    return element;
                }
                log.warn("Subquery returned invalid element id: {}", id);
            }
        }
        return null;
    }

    private void collect(Object id) {
        readCount++;
        if (currentIds != null) {
            if (currentIds.size() < maximumCachedResultSize) {
                currentIds.add(id);
            } else {
                currentIds = null;
            }
        }
    }

    /**
     * Whether the subquery read as many ids as its limit allows, so that the index may hold more. Where this iterator
     * came to its end with fewer elements than that limit, it left ids out which aren't elements of the transaction,
     * such as those of relations which the transaction removed, and a larger limit may find further elements.
     */
    @Override
    public boolean hasBackendReachedLimit() {
        return subQuery.hasLimit() && position >= subQuery.getLimit();
    }

    @Override
    public long getBackendResultsRead() {
        return position;
    }

    @Override
    public void skipBackendResults(long count) {
        Preconditions.checkArgument(count >= 0, "Can't skip a negative number of results: %s", count);
        Preconditions.checkState(!started,
            "Ids can only be skipped before the iterator looks for its first element");
        toSkip = count;
    }

    @Override
    protected JanusGraphElement computeNext() {
        JanusGraphElement next = probed;
        if (next != null) {
            probed = null;
        } else {
            next = findNext();
        }
        if (next != null) {
            //Counted as the element is returned, because the count decides whether the result is safe to cache and
            //so is part of the contract rather than an observation of it. Counted once the element is in hand, so
            //that a read which threw cannot leave the count reporting an element that was never produced
            emittedCount++;
            position = readCount;
            return next;
        }
        exhausted = true;
        position = readCount;
        close();
        return endOfData();
    }

    /**
     * Close the iterator, stop timer and update profiler.
     * Put results into cache only if no later query can ask for more results than the cached list holds.
     */
    @Override
    public void close() {
        try {
            if (isTimerRunning) {
                isTimerRunning = false;
                try {
                    //computeNext records the stream ending, which spares the probe below on the common path. The probe
                    //is still needed when it did not: a caller which took exactly as many elements as the limit
                    //allowed stops without ever driving the iterator to its end, and that result is complete and
                    //worth caching. Looking ahead is the only way to tell that apart from a caller which stopped
                    //early by choice. Ids the cache wouldn't keep aren't worth the probe. Nor is that of an iterator
                    //which skipped ids of the first index of a joint query, which has no limit: its caller, which
                    //counts the elements of the reads before it too, may have stopped it short of the iterator's own
                    //limit to read on, where the probe would read on, up to the next element or through the rest of
                    //the index where none is left, and turn ids into elements which the next read turns into elements
                    //again. An iterator over a subquery with a limit is stopped to read on only once it read all the
                    //ids which the index returned under that limit, as every index provider here returns no more than
                    //the limit, which leaves the probe nothing to read; a consumer which stops it for good has the
                    //probe read up to the next element, as for a read which skipped nothing
                    final boolean mayProbe = toSkip == 0 || subQuery.hasLimit();
                    if (currentIds != null && (exhausted || (mayProbe && !probe()))) {
                        //The probe may have read on past what the cache keeps, which drops the ids
                        final List<Object> collected = currentIds;
                        if (collected != null && isSafeToCache()) {
                            indexCache.put(subQuery, collected);
                        }
                    }
                } finally {
                    profiler.setResultSize(readCount);
                    profiler.stopTimer();
                }
            }
        } finally {
            //After the probe above, which may still read from it, and whether or not it failed: an index backend may
            //hold a resource behind its stream
            final Stream<?> stream = indexStream;
            if (stream != null) {
                indexStream = null;
                stream.close();
            }
        }
    }

    //Whether another element follows, which is kept for a read after the probe
    private boolean probe() {
        if (probed == null) {
            probed = findNext();
        }
        return probed != null;
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
     * safe to store in either of two cases. Fewer elements were emitted than {@code readLimit}: then the read limit
     * never stopped the stream, which ended where the index ran out or where the subquery's own limit stopped it, and
     * the list holds every id up to there, those skipped and those which turned into no element included. Such a list
     * is recorded against that limit, and taken for complete only where it is shorter, so a list which the
     * subquery's limit cut is served only to a query which asks for no more. Or the subquery carries a limit no wider
     * than the one the list was read under, which is what keeps a truncated prefix from being served to a query that
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
