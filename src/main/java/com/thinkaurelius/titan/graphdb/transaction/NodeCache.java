package com.thinkaurelius.titan.graphdb.transaction;

import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

public interface NodeCache {

	public boolean contains(long id);
	
	public InternalNode get(long id);		
	
	public void add(InternalNode node, long id);
    
    public Iterable<InternalNode> getAll();

    public boolean remove(long nodeid);
	
	public void close();
	
}
