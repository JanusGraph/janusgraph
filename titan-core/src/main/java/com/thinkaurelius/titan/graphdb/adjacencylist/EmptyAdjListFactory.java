package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class EmptyAdjListFactory implements AdjacencyListFactory {

    public static final EmptyAdjListFactory INSTANCE = new EmptyAdjListFactory();

    private static final AdjacencyList EMPTY = new InitialAdjacencyList(new AdjacencyListStrategy() {
        @Override
        public int extensionThreshold() {
            return 0;
        }

        @Override
        public AdjacencyList upgrade(AdjacencyList old) {
            throw new IllegalStateException("Cannot add to empty adjacency list");
        }

        @Override
        public RelationComparator getComparator() {
            throw new IllegalStateException("Cannot add to empty adjacency list");
        }

        @Override
        public AdjacencyListFactory getFactory() {
            return INSTANCE;
        }
    });

    private EmptyAdjListFactory() {
    }

    @Override
    public AdjacencyList emptyList(EdgeDirection dir) {
        return EMPTY;
    }


}
