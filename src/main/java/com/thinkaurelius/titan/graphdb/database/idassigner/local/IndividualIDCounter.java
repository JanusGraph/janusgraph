package com.thinkaurelius.titan.graphdb.database.idassigner.local;

public interface IndividualIDCounter {

	public long nextID();
	
	public boolean hasNext();
	
	public long getCurrentID();
	
	public void close();
	
}
