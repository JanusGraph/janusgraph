package com.thinkaurelius.titan.graphdb.adjacencylist;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class EmptyAdjListStrategy implements AdjacencyListStrategy {

    private final AdjacencyList INITIAL_LIST = new InitialAdjacencyList(this);

    public static final EmptyAdjListStrategy INSTANCE = new EmptyAdjListStrategy();

    private EmptyAdjListStrategy() {}

    @Override
    public AdjacencyList emptyList() {
        return INITIAL_LIST;
    }

    @Override
    public int extensionThreshold() {
        return 0;
    }

    @Override
    public AdjacencyList upgrade(AdjacencyList old) {
        throw new IllegalStateException("Cannot add to emptied adjacency list");
    }

    @Override
    public AdjacencyListStrategy getInnerStrategy() {
        throw new UnsupportedOperationException();
    }
}
