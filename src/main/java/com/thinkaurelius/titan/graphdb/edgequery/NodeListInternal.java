package com.thinkaurelius.titan.graphdb.edgequery;

import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.core.NodeIDList;

public interface NodeListInternal extends NodeIDList {

	public void add(Node n);
	
}
