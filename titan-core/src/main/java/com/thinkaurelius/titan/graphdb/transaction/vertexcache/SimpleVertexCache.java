package com.thinkaurelius.titan.graphdb.transaction.vertexcache;

import com.carrotsearch.hppc.LongObjectMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectContainer;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SimpleVertexCache implements VertexCache {

    private static final int defaultCacheSize = 10;

    private final LongObjectMap map;

    public SimpleVertexCache() {
        map = new LongObjectOpenHashMap(defaultCacheSize);
    }

    @Override
    public boolean contains(long id) {
        return map.containsKey(id);
    }

    @Override
    public InternalVertex get(long id) {
        return (InternalVertex) map.get(id);
    }

    @Override
    public void add(InternalVertex vertex, long id) {
        Preconditions.checkNotNull(vertex);
        Preconditions.checkArgument(id > 0, "Vertex id must be positive");
        assert !map.containsKey(id);
        map.put(id, vertex);
    }

    @Override
    public Iterable<InternalVertex> getAll() {
        ObjectContainer oc = map.values();
        return Iterables.transform(oc,new Function() {
            @Nullable
            @Override
            public InternalVertex apply(@Nullable Object o) {
                return (InternalVertex)o;
            }
        });
    }

    @Override
    public boolean remove(long vertexid) {
        return map.remove(vertexid)!=null;
    }


    @Override
    public synchronized void close() {
        map.clear();
    }


}
