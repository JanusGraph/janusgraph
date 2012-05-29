package com.thinkaurelius.titan.graphdb.transaction;

import cern.colt.list.ObjectArrayList;
import cern.colt.map.OpenLongObjectHashMap;
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
		boolean contains = super.containsKey(id);
		readLock.unlock();
		return contains;
	}
	
	@Override
	public InternalTitanVertex get(long id) {
		readLock.lock();
		InternalTitanVertex node = (InternalTitanVertex)super.get(Long.valueOf(id));
		readLock.unlock();
		return node;
	}
	
	@Override
	public void add(InternalTitanVertex vertex, long id) {
		writeLock.lock();
		assert !containsKey(Long.valueOf(id));
		put(id, vertex);
		writeLock.unlock();
	}

    @Override
    public Iterable<InternalTitanVertex> getAll() {
        ArrayList<InternalTitanVertex> vertices = new ArrayList<InternalTitanVertex>(super.size()+2);
        readLock.lock();
        ObjectArrayList all = super.values();
        for (int i=0;i<all.size();i++) vertices.add((InternalTitanVertex)all.get(i));
        readLock.unlock();
        return vertices;
    }
    
    @Override
    public boolean remove(long vertexid) {
        writeLock.lock();
        boolean removed = super.removeKey(vertexid);
        writeLock.unlock();
        return removed;
    }


    @Override
	public synchronized void close() {
		clear();
	}


	
}
