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

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.util.ConcurrentLRUCache;
import org.janusgraph.util.datastructures.Retriever;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;


public class LRUVertexCache implements VertexCache {

    private final NonBlockingHashMapLong<InternalVertex> volatileVertices;
    private final ConcurrentLRUCache<InternalVertex> cache;

    public LRUVertexCache(int capacity) {
        volatileVertices = new NonBlockingHashMapLong<InternalVertex>();
        cache = new ConcurrentLRUCache<InternalVertex>(capacity * 2, // upper is double capacity
                capacity + capacity / 3, // lower is capacity + 1/3
                capacity, // acceptable watermark is capacity
                100, true, false, // 100 items initial size + use only one thread for items cleanup
                new ConcurrentLRUCache.EvictionListener<InternalVertex>() {
                    @Override
                    public void evictedEntry(Long vertexId, InternalVertex vertex) {
                        if (vertexId == null || vertex == null)
                            return;

                        if (vertex.isModified()) {
                            volatileVertices.putIfAbsent(vertexId, vertex);
                        }
                    }
                });

        cache.setAlive(true); //need counters to its actually LRU
    }

    @Override
    public boolean contains(long id) {
        Long vertexId = id;
        return cache.containsKey(vertexId) || volatileVertices.containsKey(vertexId);
    }

    @Override
    public InternalVertex get(long id, final Retriever<Long, InternalVertex> retriever) {
        final Long vertexId = id;

        InternalVertex vertex = cache.get(vertexId);

        if (vertex == null) {
            InternalVertex newVertex = volatileVertices.get(vertexId);

            if (newVertex == null) {
                newVertex = retriever.get(vertexId);
            }

            vertex = cache.putIfAbsent(vertexId, newVertex);
            if (vertex == null)
                vertex = newVertex;
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
        List<InternalVertex> vertices = new ArrayList<InternalVertex>(10);
        for (InternalVertex v : volatileVertices.values()) {
            if (v.isNew()) vertices.add(v);
        }
        return vertices;
    }


    @Override
    public synchronized void close() {
        volatileVertices.clear();
        cache.destroy();
    }
}
