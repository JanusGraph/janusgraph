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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.query.profile.QueryProfiler;

import org.janusgraph.graphdb.transaction.subquerycache.GuavaSubqueryCache;
import org.janusgraph.graphdb.transaction.subquerycache.SubqueryCache;

/**
 * @author davidclement90@laposte.net
 */
public class SubqueryIterator extends CloseableAbstractIterator<JanusGraphElement> {

    private final JointIndexQuery.Subquery subQuery;

    private final SubqueryCache indexCache;

    private Iterator<? extends JanusGraphElement> elementIterator;

    private List<Object> currentIds;

    private QueryProfiler profiler;

    private boolean isTimerRunning;

    public SubqueryIterator(JointIndexQuery.Subquery subQuery, IndexSerializer indexSerializer, BackendTransaction tx,
                            SubqueryCache indexCache, int limit,
                            Function<Object, ? extends JanusGraphElement> function, List<Object> otherResults) {
        this.subQuery = subQuery;
        this.indexCache = indexCache;
        profiler = subQuery.getProfiler();
        // if subquery's profiler is flattened out (merged with its parent because it's the only child), we don't start
        // its timer here because it shares its timer with its parent and the timer has already been started
        if (!subQuery.isProfilerFlattened()) {
            profiler.startTimer();
            isTimerRunning = true;
        }
        final List<Object> cacheResponse = indexCache.getIfPresent(subQuery);
        final Stream<?> stream;
        if (cacheResponse != null) {
            stream = cacheResponse.stream();
            profiler.setAnnotation(QueryProfiler.CACHED_ANNOTATION, true);
        } else {
            profiler.setAnnotation(QueryProfiler.CACHED_ANNOTATION, false);
            try {
                currentIds = new ArrayList<>();
                stream = indexSerializer.query(subQuery, tx).peek(r -> currentIds.add(r));
            } catch (final Exception e) {
                throw new JanusGraphException("Could not call index", e);
            }
        }
        elementIterator = stream.filter(e -> otherResults == null || otherResults.contains(e)).limit(limit).map(function).map(r -> (JanusGraphElement) r).iterator();
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
        if (currentIds != null) {
            if (!elementIterator.hasNext()) {
                indexCache.put(subQuery, currentIds);
            }
            profiler.setResultSize(currentIds.size());
            currentIds = null;
        }
        if (isTimerRunning) {
            profiler.stopTimer();
            isTimerRunning = false;
        }
    }

}
