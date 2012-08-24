package com.thinkaurelius.titan.graphdb.transaction;

import cern.colt.list.ObjectArrayList;
import cern.colt.map.OpenLongObjectHashMap;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StandardVertexCache extends OpenLongObjectHashMap implements VertexCache {
	
	private static final long serialVersionUID = 1609323162410880217L;
	private static final int defaultCacheSize = 10;
	
	ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
	Lock readLock = rwl.readLock();
	Lock writeLock = rwl.writeLock();
	
	public StandardVertexCache() {
		super(defaultCacheSize);
	}

	
	@Override
	public boolean contains(long id) {
		readLock.lock();
        try {
    		return super.containsKey(id);
        } finally {
		    readLock.unlock();
        }
	}
	
	@Override
	public InternalTitanVertex get(long id) {
		readLock.lock();
        try {
		    return (InternalTitanVertex)super.get(id);
        } finally {
		    readLock.unlock();
        }
	}
	
	@Override
	public void add(InternalTitanVertex vertex, long id) {
        Preconditions.checkNotNull(vertex);
        Preconditions.checkArgument(id>0,"Vertex id must be positive");
		writeLock.lock();
        try {
            assert !containsKey(id);
            put(id, vertex);
        } finally {
		    writeLock.unlock();
        }
	}

    @Override
    public Iterable<InternalTitanVertex> getAll() {
        ArrayList<InternalTitanVertex> vertices = new ArrayList<InternalTitanVertex>(super.size()+2);
        readLock.lock();
        try {
            ObjectArrayList all = super.values();
            for (int i=0;i<all.size();i++) vertices.add((InternalTitanVertex)all.get(i));
        } finally {
            readLock.unlock();
        }
        return vertices;
    }
    
    @Override
    public boolean remove(long vertexid) {
        writeLock.lock();
        try {
            return super.removeKey(vertexid);
        } finally {
            writeLock.unlock();
        }
    }


    @Override
	public synchronized void close() {
		clear();
	}


	
}
