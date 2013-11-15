package com.thinkaurelius.titan.graphdb.transaction.vertexcache;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.util.ConcurrentLRUCache;
import com.thinkaurelius.titan.util.datastructures.Retriever;
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

                        if (vertex.hasAddedRelations()) {
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