package com.thinkaurelius.titan.graphdb.transaction;

import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;

public interface VertexCache {

	public boolean contains(long id);
	
	public InternalTitanVertex get(long id);
	
	public void add(InternalTitanVertex vertex, long id);
    
    public Iterable<InternalTitanVertex> getAll();

    public boolean remove(long vertexid);
	
	public void close();
	
}
