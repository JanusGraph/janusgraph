package com.thinkaurelius.titan.graphdb.transaction.vertexcache;

import com.carrotsearch.hppc.ObjectContainer;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Preconditions;
import com.google.common.cache.*;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.util.datastructures.Retriever;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

public class LRUVertexCache implements VertexCache {

    private final ConcurrentMap<Long, InternalVertex> addedRelVertices;
    private final Cache<Long, InternalVertex> cache;

    public LRUVertexCache(final long capacity, final int concurrencyLevel) {
        addedRelVertices = new ConcurrentHashMap<Long, InternalVertex>(8, 0.75f, concurrencyLevel);
        cache = CacheBuilder.newBuilder().maximumSize(capacity).concurrencyLevel(concurrencyLevel)
                .removalListener(new RemovalListener<Long, InternalVertex>() {
                    @Override
                    public void onRemoval(RemovalNotification<Long, InternalVertex> notification) {
                        //Should only get evicted based on size constraint
                        Preconditions.checkArgument(notification.getCause() == RemovalCause.SIZE || notification.getCause() == RemovalCause.EXPLICIT);
                        InternalVertex v = notification.getValue();
                        if (v.hasAddedRelations()) {
                            addedRelVertices.putIfAbsent(notification.getKey(), v);
                        }
                    }
                })
                .build();
    }

    @Override
    public boolean contains(long id) {
        Long lid = Long.valueOf(id);
        return addedRelVertices.containsKey(lid) || cache.getIfPresent(lid) != null;
    }

    @Override
    public InternalVertex get(final long id, final Retriever<Long, InternalVertex> constructor) {
        final Long lid = Long.valueOf(id);
        InternalVertex v = addedRelVertices.get(lid);
        if (v != null) return v;
        try {
            return cache.get(lid, new Callable<InternalVertex>() {
                @Override
                public InternalVertex call() throws Exception {
                    InternalVertex v = constructor.get(lid);
                    Preconditions.checkNotNull(v);
                    return v;
                }
            });
        } catch (ExecutionException e) {
            throw new RuntimeException("Exception while retrieving vertex", e.getCause());
        }
    }

    @Override
    public void add(InternalVertex vertex, long id) {
        Preconditions.checkNotNull(vertex);
        Preconditions.checkArgument(id != 0);
        final Long lid = Long.valueOf(id);
        if (vertex.isNew() || vertex.hasAddedRelations()) {
            addedRelVertices.putIfAbsent(lid, vertex);
        } else {
            cache.put(Long.valueOf(id), vertex);
        }
    }

    @Override
    public List<InternalVertex> getAllNew() {
        ArrayList<InternalVertex> vertices = new ArrayList<InternalVertex>(10);
        for (InternalVertex v : addedRelVertices.values()) {
            if (v.isNew()) vertices.add(v);
        }
        return vertices;
    }


    @Override
    public synchronized void close() {
        cache.cleanUp();
        cache.invalidateAll();
    }


}
