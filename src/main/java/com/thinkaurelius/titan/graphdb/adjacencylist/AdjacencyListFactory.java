package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface AdjacencyListFactory {

    public AdjacencyList emptyList(EdgeDirection dir);

}
