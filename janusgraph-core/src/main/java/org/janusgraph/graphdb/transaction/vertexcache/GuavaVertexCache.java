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

package org.janusgraph.graphdb.transaction.vertexcache;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.vertices.AbstractVertex;
import org.janusgraph.util.datastructures.Retriever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

public class GuavaVertexCache implements VertexCache {

    private static final Logger log =
            LoggerFactory.getLogger(GuavaVertexCache.class);

    private final ConcurrentMap<Long, InternalVertex> volatileVertices;
    private final Cache<Long, InternalVertex> cache;

    public GuavaVertexCache(final long maxCacheSize, final int concurrencyLevel, final int initialDirtySize) {
        volatileVertices = new NonBlockingHashMapLong<>(initialDirtySize);
        log.debug("Created dirty vertex map with initial size {}", initialDirtySize);

        cache = CacheBuilder.newBuilder().maximumSize(maxCacheSize).concurrencyLevel(concurrencyLevel)
                .removalListener((RemovalListener<Long, InternalVertex>) notification -> {
                    if (notification.getCause() == RemovalCause.EXPLICIT) { //Due to invalidation at the end
                        assert volatileVertices.isEmpty();
                        return;
                    }
                    //Should only get evicted based on size constraint or replaced through add
                    assert (notification.getCause() == RemovalCause.SIZE || notification.getCause() == RemovalCause.REPLACED) : "Cause: " + notification.getCause();
                    final InternalVertex v = notification.getValue();
                    if (((AbstractVertex) v).isTxOpen() && (v.isModified() || v.isRemoved())) {
                        volatileVertices.putIfAbsent(notification.getKey(), v);
                    }
                })
                .build();
        log.debug("Created vertex cache with max size {}", maxCacheSize);
    }

    @Override
    public boolean contains(long id) {
        Long vertexId = id;
        return cache.getIfPresent(vertexId) != null || volatileVertices.containsKey(vertexId);
    }

    @Override
    public InternalVertex get(final long id, final Retriever<Long, InternalVertex> retriever) {
        final Long vertexId = id;

        InternalVertex vertex = cache.getIfPresent(vertexId);

        if (vertex == null) {
            InternalVertex newVertex = volatileVertices.get(vertexId);

            if (newVertex == null) {
                newVertex = retriever.get(vertexId);
            }
            assert newVertex!=null;
            try {
                vertex = cache.get(vertexId, new NewVertexCallable(newVertex));
            } catch (Exception e) { throw new AssertionError("Should not happen: "+e.getMessage()); }
            assert vertex!=null;
        }

        return vertex;
    }

    @Override
    public void add(InternalVertex vertex, long id) {
        Preconditions.checkNotNull(vertex);
        Preconditions.checkArgument(id != 0);
        Long vertexId = id;

        cache.put(vertexId, vertex);
        if (vertex.isNew() || vertex.hasAddedRelations())
            volatileVertices.put(vertexId, vertex);
    }

    @Override
    public List<InternalVertex> getAllNew() {
        final List<InternalVertex> vertices = new ArrayList<>(10);
        for (InternalVertex v : volatileVertices.values()) {
            if (v.isNew()) vertices.add(v);
        }
        return vertices;
    }

    @Override
    public synchronized void close() {
        volatileVertices.clear();
        cache.invalidateAll();
        cache.cleanUp();
    }

    private static class NewVertexCallable implements Callable<InternalVertex> {

        private final InternalVertex vertex;

        private NewVertexCallable(InternalVertex vertex) {
            this.vertex = vertex;
        }

        @Override
        public InternalVertex call() {
            return vertex;
        }
    }
}
