package com.thinkaurelius.titan.graphdb.util;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.Edge;
import com.thinkaurelius.titan.core.Node;

import java.util.Collection;

public class Relations {

	public static final Collection<? extends Node> getUniqueNodes(Edge e) {
		Collection<? extends Node> nodes = e.getNodes();
		if (nodes.size()>1)
			return ImmutableSet.copyOf(e.getNodes());
		else return nodes;
	}

}
