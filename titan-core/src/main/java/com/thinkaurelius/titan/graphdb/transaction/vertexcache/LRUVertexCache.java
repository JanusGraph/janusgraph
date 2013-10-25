package com.thinkaurelius.titan.graphdb.transaction.vertexcache;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.util.datastructures.Retriever;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

public class LRUVertexCache implements VertexCache {
    private final NonBlockingHashMapLong<InternalVertex> volatileVertices;
    //private final LoadingCache<Long, InternalVertex> cache;

    public LRUVertexCache(final long capacity, final int concurrencyLevel) {
        volatileVertices = new NonBlockingHashMapLong<InternalVertex>();

        /*
        cache = CacheBuilder.newBuilder()
                            .maximumSize(capacity)
                            .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                            .removalListener(RemovalListeners.asynchronous(new RemovalListener<Long, InternalVertex>() {
                                @Override
                                public void onRemoval(RemovalNotification<Long, InternalVertex> notification) {
                                    if (notification.getKey() == null || notification.getValue() == null)
                                        return;

                                    InternalVertex v = notification.getValue();
                                    if (v.hasAddedRelations()) {
                                        volatileVertices.putIfAbsent(notification.getKey(), v);
                                    }
                                }
                            }, Executors.newFixedThreadPool(concurrencyLevel)))
                            .build(new CacheLoader<Long, InternalVertex>() {
                                @Override
                                public InternalVertex load(Long vertexId) throws Exception {
                                    InternalVertex newVertex = volatileVertices.get(vertexId);
                                    return (newVertex == null) ? vertexRetriever.get(vertexId) : newVertex;
                                }
                            });*/
    }

    @Override
    public boolean contains(long id) {
        return volatileVertices.containsKey(id);// || cache.getIfPresent(id) != null;
    }

    @Override
    public InternalVertex get(final long id, final Retriever<Long, InternalVertex> retriever) {
        //return cache.getUnchecked(id);

        Long vertexId = Long.valueOf(id);
        InternalVertex vertex = volatileVertices.get(vertexId);

        if (vertex == null) {
            InternalVertex newVertex = retriever.get(vertexId);
            vertex = volatileVertices.putIfAbsent(vertexId, newVertex);
            if (vertex == null)
                vertex = newVertex;
        }

        return vertex;

        /*try {
            return cache.get(id, new Callable<InternalVertex>() {
                @Override
                public InternalVertex call() throws Exception {
                    InternalVertex newVertex = volatileVertices.get(id);
                    return (newVertex == null) ? constructor.get(id) : newVertex;
                }
            });
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }*/
        /*
        InternalVertex v = addedRelVertices.get(id);

        if (v == null) {
            try {
                return cache.get(id, new Callable<InternalVertex>() {
                    @Override
                    public InternalVertex call() throws Exception {
                        return constructor.get(id);
                    }
                });
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        return v;*/
    }

    @Override
    public void add(InternalVertex vertex, long vertexId) {
        Preconditions.checkNotNull(vertex);
        Preconditions.checkArgument(vertexId != 0);

        //if (vertex.isNew() || vertex.hasAddedRelations()) {
            volatileVertices.put(vertexId, vertex);
        //}
    }

    @Override
    public List<InternalVertex> getAllNew() {
        ArrayList<InternalVertex> vertices = new ArrayList<InternalVertex>(10);
        for (InternalVertex v : volatileVertices.values()) {
            if (v.isNew())
                vertices.add(v);
        }
        return vertices;
    }


    @Override
    public synchronized void close() {
        volatileVertices.clear();
        //cache.invalidateAll();
    }
}
