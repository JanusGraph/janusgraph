package com.thinkaurelius.titan.graphdb.adjacencylist;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface AdjacencyListStrategy {

    public int extensionThreshold();

    public AdjacencyList upgrade(AdjacencyList old);

    public RelationComparator getComparator();

    public AdjacencyListFactory getFactory();

}
