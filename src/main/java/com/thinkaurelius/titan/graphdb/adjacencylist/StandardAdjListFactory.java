package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class StandardAdjListFactory implements AdjacencyListFactory {

    public static final StandardAdjListFactory INSTANCE = new StandardAdjListFactory();

    private StandardAdjListFactory() {  }

    @Override
    public AdjacencyList emptyList(EdgeDirection dir) {
        switch (dir) {
            case IN: return StandardAdjListStrategy.INSTANCE_IN.emptyList();
            case OUT: return StandardAdjListStrategy.INSTANCE_OUT.emptyList();
            default: throw new IllegalArgumentException(dir.toString());
        }
    }

    private static class StandardAdjListStrategy implements AdjacencyListStrategy {


        private static final StandardAdjListStrategy INSTANCE_OUT = new StandardAdjListStrategy(EdgeDirection.OUT);
        private static final StandardAdjListStrategy INSTANCE_IN = new StandardAdjListStrategy(EdgeDirection.IN);

        private final int maxArraySize = 10;
        private final EdgeDirection direction;
        private final RelationComparator relationComparator;

        private final AdjacencyList initialList = new InitialAdjacencyList(this);


        private final AdjacencyListStrategy array2SetStrategy = new AdjacencyListStrategy() {

            @Override
            public AdjacencyListFactory getFactory() {
                return StandardAdjListFactory.INSTANCE;
            }

            @Override
            public int extensionThreshold() {
                return maxArraySize;
            }

            @Override
            public AdjacencyList upgrade(AdjacencyList old) {
                return new SetAdjacencyList(setStrategy,old);
            }

            @Override
            public RelationComparator getComparator() {
                return relationComparator;
            }

        };

        private final AdjacencyListStrategy setStrategy = new AdjacencyListStrategy() {

            @Override
            public AdjacencyListFactory getFactory() {
                return StandardAdjListFactory.INSTANCE;
            }

            @Override
            public int extensionThreshold() {
                return Integer.MAX_VALUE;
            }

            @Override
            public AdjacencyList upgrade(AdjacencyList old) {
                throw new UnsupportedOperationException();
            }

            @Override
            public RelationComparator getComparator() {
                return relationComparator;
            }

        };

        private StandardAdjListStrategy(final EdgeDirection dir) {
            this.direction=dir;
            switch (dir) {
                case IN:
                    this.relationComparator = RelationComparator.IN;
                    break;
                case OUT:
                    this.relationComparator = RelationComparator.OUT;
                    break;
                default: throw new IllegalArgumentException(dir.toString());
            }
        }

        public AdjacencyList emptyList() {
            return initialList;
        }

        @Override
        public int extensionThreshold() {
            return 0;
        }

        @Override
        public AdjacencyList upgrade(AdjacencyList old) {
            Preconditions.checkArgument(old == null || old.isEmpty(), "Expected empty adjacency list");
            return new ArrayAdjacencyList(array2SetStrategy);
        }

        @Override
        public RelationComparator getComparator() {
            return relationComparator;
        }

        @Override
        public AdjacencyListFactory getFactory() {
            return StandardAdjListFactory.INSTANCE;
        }

        public static StandardAdjListStrategy getInstance(EdgeDirection dir) {
            switch (dir) {
                case IN: return INSTANCE_IN;
                case OUT: return INSTANCE_OUT;
                default: throw new IllegalArgumentException(dir.toString());
            }
        }
    }
}
