package com.thinkaurelius.titan.graphdb.adjacencylist;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface AdjacencyListStrategy {

    public AdjacencyList emptyList();
    
    public int extensionThreshold();
    
    public AdjacencyList upgrade(AdjacencyList old);

    public AdjacencyListStrategy getInnerStrategy();
    
}
