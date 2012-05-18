package com.thinkaurelius.titan.graphdb.edgequery;

import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.VertexList;

public interface VertexListInternal extends VertexList {

	public void add(TitanVertex n);
    
    public void addAll(VertexList nodes);
	
}
