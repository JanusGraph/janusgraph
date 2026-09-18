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

    public SubqueryIterator(JointIndexQuery.Subquery subQuery, IndexSerializer indexSerializer,
                            BackendTransaction backendTx,
                            StandardJanusGraphTx tx,
                            SubqueryCache indexCache, int limit,
                            Function<Object, ? extends JanusGraphElement> function, List<Object> otherResults) {
        this.subQuery = subQuery;
        this.indexCache = indexCache;
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
        //complete. Scanning the list for each element would make the intersection cost O(n*m)
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
                .limit(limit)
                .iterator();
    }

    @Override
    protected JanusGraphElement computeNext() {
        if (elementIterator.hasNext()) {
            return elementIterator.next();
        }
        close();
        return endOfData();
    }

    /**
     * Close the iterator, stop timer and update profiler.
     * Put results into cache if the underlying elementIterator is exhausted.
     */
    @Override
    public void close() {
        if (isTimerRunning) {
            assert currentIds != null;
            if (!elementIterator.hasNext()) {
                indexCache.put(subQuery, currentIds);
            }
            profiler.setResultSize(currentIds.size());
            profiler.stopTimer();
            isTimerRunning = false;
        }
    }

}
