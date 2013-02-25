package com.thinkaurelius.titan.graphdb.transaction.vertexcache;

import com.carrotsearch.hppc.LongObjectMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectContainer;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.util.datastructures.Retriever;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConcurrentVertexCache implements VertexCache {

    private static final int defaultCacheSize = 10;

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock readLock = rwl.readLock();
    private final Lock writeLock = rwl.writeLock();
    private final LongObjectMap map;

    public ConcurrentVertexCache() {
        map = new LongObjectOpenHashMap(defaultCacheSize);
    }


    @Override
    public boolean contains(long id) {
        readLock.lock();
        try {
            return map.containsKey(id);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public InternalVertex get(long id, Retriever<Long,InternalVertex> constructor) {
        InternalVertex v = null;
        readLock.lock();
        try {
            v = (InternalVertex) map.get(id);
        } finally {
            readLock.unlock();
        }
        if (v==null) {
            writeLock.lock();
            try {
                v = (InternalVertex) map.get(id);
                if (v==null) {
                    v = constructor.get(id);
                    Preconditions.checkNotNull(v);
                    map.put(id,v);
                }
            } finally {
                writeLock.unlock();
            }
        }
        return v;
    }

    @Override
    public void add(InternalVertex vertex, long id) {
        Preconditions.checkNotNull(vertex);
        Preconditions.checkArgument(id > 0, "Vertex id must be positive");
        writeLock.lock();
        try {
            assert !map.containsKey(id);
            map.put(id, vertex);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Iterable<InternalVertex> getAll() {
        ArrayList<InternalVertex> vertices = new ArrayList<InternalVertex>(map.size() + 2);
        readLock.lock();
        try {
            ObjectContainer oc = map.values();
            for (Object o : oc) {
                vertices.add((InternalVertex)o);
            }
        } finally {
            readLock.unlock();
        }
        return vertices;
    }


    @Override
    public synchronized void close() {
        map.clear();
    }


}
