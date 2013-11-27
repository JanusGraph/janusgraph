package com.thinkaurelius.titan.graphdb.transaction.vertexcache;

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
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

public class GuavaVertexCache implements VertexCache {

    private final ConcurrentMap<Long, InternalVertex> volatileVertices;
    private final Cache<Long, InternalVertex> cache;

    public GuavaVertexCache(final long capacity, final int concurrencyLevel) {
//        volatileVertices = new ConcurrentHashMap<Long, InternalVertex>(8, 0.75f, concurrencyLevel);
        volatileVertices = new NonBlockingHashMapLong<InternalVertex>();

        cache = CacheBuilder.newBuilder().maximumSize(capacity).concurrencyLevel(concurrencyLevel)
                .removalListener(new RemovalListener<Long, InternalVertex>() {
                    @Override
                    public void onRemoval(RemovalNotification<Long, InternalVertex> notification) {
                        if (notification.getCause() == RemovalCause.EXPLICIT) { //Due to invalidation at the end
                            assert volatileVertices.isEmpty();
                            return;
                        }
                        //Should only get evicted based on size constraint or replaced through add
                        assert (notification.getCause() == RemovalCause.SIZE || notification.getCause() == RemovalCause.REPLACED) : "Cause: " + notification.getCause();
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
        List<InternalVertex> vertices = new ArrayList<InternalVertex>(10);
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