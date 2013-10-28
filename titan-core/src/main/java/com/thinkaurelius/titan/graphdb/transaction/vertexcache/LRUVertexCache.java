package com.thinkaurelius.titan.graphdb.transaction.vertexcache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Preconditions;
import com.google.common.cache.*;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.util.datastructures.Retriever;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

public class LRUVertexCache implements VertexCache {
    private final NonBlockingHashMapLong<InternalVertex> volatileVertices;
    private final Cache<Long, InternalVertex> cache;

    public LRUVertexCache(long capacity, int concurrencyLevel) {
        volatileVertices = new NonBlockingHashMapLong<InternalVertex>();
        cache = CacheBuilder.newBuilder()
                            .maximumSize(capacity)
                            .concurrencyLevel(concurrencyLevel)
                            .removalListener(new RemovalListener<Long, InternalVertex>() {
                                @Override
                                public void onRemoval(RemovalNotification<Long, InternalVertex> notification) {
                                    if (notification.getKey() == null || notification.getValue() == null)
                                        return;

                                    InternalVertex v = notification.getValue();
                                    if (v.hasAddedRelations()) {
                                        volatileVertices.putIfAbsent(notification.getKey(), v);
                                    }
                                }
                            })
                            .build();
    }

    @Override
    public boolean contains(long id) {
        Long vertexId = id;
        return cache.getIfPresent(vertexId) != null || volatileVertices.containsKey(vertexId);
    }

    @Override
    public InternalVertex get(long id, final Retriever<Long, InternalVertex> retriever) {
        final Long vertexId = id;

        try {
            return cache.get(vertexId, new Callable<InternalVertex>() {
                @Override
                public InternalVertex call() throws Exception {
                    InternalVertex newVertex = volatileVertices.get(vertexId);
                    return (newVertex == null) ? retriever.get(vertexId) : newVertex;
                }
            });
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
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
            if (v.isNew())
                vertices.add(v);
        }
        return vertices;
    }


    @Override
    public synchronized void close() {
        volatileVertices.clear();
        cache.invalidateAll();
    }
}
