package com.thinkaurelius.titan.graphdb.edgequery;

import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.core.NodeList;

public interface NodeListInternal extends NodeList {

	public void add(Node n);
	
}
