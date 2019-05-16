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

import com.google.common.cache.Cache;

/**
 * @author davidclement90@laposte.net
 */
public class SubqueryIterator implements Iterator<JanusGraphElement>, AutoCloseable {

    private final JointIndexQuery.Subquery subQuery;

    private final Cache<JointIndexQuery.Subquery, List<Object>> indexCache;

    private Iterator<? extends JanusGraphElement> elementIterator;

    private List<Object> currentIds;

    private QueryProfiler profiler;

    private boolean isTimerRunning;

    public SubqueryIterator(JointIndexQuery.Subquery subQuery, IndexSerializer indexSerializer, BackendTransaction tx,
            Cache<JointIndexQuery.Subquery, List<Object>> indexCache, int limit,
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
                stream = indexSerializer.query(subQuery, tx).peek(r -> currentIds.add(r));
            } catch (final Exception e) {
                throw new JanusGraphException("Could not call index", e.getCause());
            }
        }
        elementIterator = stream.filter(e -> otherResults == null || otherResults.contains(e)).limit(limit).map(function).map(r -> (JanusGraphElement) r).iterator();
    }

    @Override
    public boolean hasNext() {
        if (!elementIterator.hasNext() && currentIds != null) {
            indexCache.put(subQuery, currentIds);
            profiler.stopTimer();
            isTimerRunning = false;
            profiler.setResultSize(currentIds.size());
        }
        return elementIterator.hasNext();
    }

    @Override
    public JanusGraphElement next() {
        return this.elementIterator.next();
    }

    @Override
    public void close() throws Exception {
        if (isTimerRunning) {
            profiler.stopTimer();
        }
    }

}
