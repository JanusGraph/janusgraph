package com.thinkaurelius.titan.graphdb.database.idassigner.local;

import com.thinkaurelius.titan.graphdb.database.serialize.ObjectDiskStorage;

import java.util.NoSuchElementException;

public class LocalID implements IndividualIDCounter {

	private static final int increment = 5000;
	
	private long currentcount;
	private long countlimit;
	
	private final long bitOffset;
	private final long inserterID;
	private final long maxID;
	
	private final ObjectDiskStorage objectStore;
	private final String configFile;
	
	public LocalID(long maxID, long maxNoInserter, long inserterID, ObjectDiskStorage objectStore, String configfile) {
		this.objectStore = objectStore;
		configFile = configfile;
		
		this.maxID=maxID;
		if (inserterID>=maxNoInserter) throw new IllegalArgumentException("Inserter ID exceeds maximum.");
		this.inserterID=inserterID;
		bitOffset = (long)Math.ceil(Math.log(maxNoInserter)/Math.log(2));
		if ((1L<<(bitOffset+2))> maxID) throw new IllegalArgumentException("Bit Offset already exceeds maximum ID allowed");
		
		currentcount = objectStore.getObject(configFile, 0l);
		countlimit = currentcount;
	}
	
	private long makeID(long count) {
		return (count<<bitOffset) | inserterID;
	}
	
	@Override
	public synchronized boolean hasNext() {
		return makeID(currentcount+1)<maxID;
	}
	
	@Override
	public synchronized long nextID() {
		if (currentcount<countlimit) {
			long id = makeID(++currentcount);
			if (id>=maxID) throw new NoSuchElementException("Current ID exceeds the maximum!");
			return id;
		} else {
			nextIDBlock();
			return nextID();
		}
	}
	
	@Override
	public synchronized long getCurrentID() {
		return makeID(currentcount);
	}
	
	private void nextIDBlock() {
		assert currentcount == countlimit;
		countlimit += increment;
		objectStore.putObject(configFile, countlimit);
	}
	
	@Override
	public synchronized void close() {
		countlimit = currentcount;
		objectStore.putObject(configFile, countlimit);
	}
	
}
