package com.thinkaurelius.titan.graphdb.transaction;

import cern.colt.map.OpenLongObjectHashMap;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StandardNodeCache extends OpenLongObjectHashMap implements NodeCache {
	
	private static final long serialVersionUID = 1609323162410880217L;
	private static final int defaultCacheSize = 10;
	
	ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
	Lock readLock = rwl.readLock();
	Lock writeLock = rwl.writeLock();
	
	public StandardNodeCache() {
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
	public InternalNode get(long id) {
		readLock.lock();
		InternalNode node = (InternalNode)super.get(Long.valueOf(id));
		readLock.unlock();
		return node;
	}
	
	@Override
	public void add(InternalNode node, long id) {
		writeLock.lock();
		assert !containsKey(Long.valueOf(id));
		put(id, node);
		writeLock.unlock();
	}

	
	@Override
	public synchronized void close() {
		clear();
	}


	
}
