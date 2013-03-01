package com.thinkaurelius.titan.graphdb.transaction.vertexcache;

import com.carrotsearch.hppc.LongObjectMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectContainer;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.util.datastructures.Retriever;

import javax.annotation.Nullable;
import java.util.ArrayList;

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
    public InternalVertex get(long id, Retriever<Long,InternalVertex> constructor) {
        InternalVertex v = (InternalVertex) map.get(id);
        if (v==null) {
            v = constructor.get(id);
            Preconditions.checkNotNull(v);
            map.put(id,v);
        }
        return v;
    }

    @Override
    public void add(InternalVertex vertex, long id) {
        Preconditions.checkNotNull(vertex);
        Preconditions.checkArgument(id!=0);
        Preconditions.checkArgument(!map.containsKey(id),id);
        map.put(id, vertex);
    }

    @Override
    public Iterable<InternalVertex> getAll() {
        ArrayList<InternalVertex> vertices = new ArrayList<InternalVertex>(map.size() + 2);
        ObjectContainer<InternalVertex> oc = map.values();
        for (ObjectCursor<InternalVertex> o : oc) {
            vertices.add(o.value);
        }
        return vertices;
    }


    @Override
    public synchronized void close() {
        map.clear();
    }


}
