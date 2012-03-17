package com.thinkaurelius.titan.graphdb.transaction;

import cern.colt.list.ObjectArrayList;
import cern.colt.map.OpenLongObjectHashMap;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

import java.util.ArrayList;
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
    public Iterable<InternalNode> getAll() {
        ArrayList<InternalNode> nodes = new ArrayList<InternalNode>(super.size()+2);
        readLock.lock();
        ObjectArrayList all = super.values();
        for (int i=0;i<all.size();i++) nodes.add((InternalNode)all.get(i));
        readLock.unlock();
        return nodes;
    }
    
    @Override
    public boolean remove(long nodeid) {
        writeLock.lock();
        boolean removed = super.removeKey(nodeid);
        writeLock.unlock();
        return removed;
    }


    @Override
	public synchronized void close() {
		clear();
	}


	
}
