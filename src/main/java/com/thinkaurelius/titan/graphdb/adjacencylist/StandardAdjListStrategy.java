package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.google.common.base.Preconditions;
import com.sun.org.apache.bcel.internal.generic.INSTANCEOF;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class StandardAdjListStrategy implements AdjacencyListStrategy {
    
    private final AdjacencyList INITIAL_LIST = new InitialAdjacencyList(this); 
    
    private static final StandardAdjListStrategy INSTANCE = new StandardAdjListStrategy();
    
    private final int initialArraySize = 5;
    private final int array2Map = 20;
    private final int array2Set = 20;

    private final AdjacencyListStrategy array2MapStrategy = new AdjacencyListStrategy() {
        @Override
        public AdjacencyList emptyList() {
            return INITIAL_LIST;
        }

        @Override
        public int extensionThreshold() {
            return array2Map;
        }

        @Override
        public AdjacencyList upgrade(AdjacencyList old) {
            return new TypedAdjacencyList(mapStrategy,old);
        }

        @Override
        public AdjacencyListStrategy getInnerStrategy() {
            throw new UnsupportedOperationException();
        }
    };

    private final AdjacencyListStrategy mapStrategy = new AdjacencyListStrategy() {
        @Override
        public AdjacencyList emptyList() {
            return INITIAL_LIST;
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
        public AdjacencyListStrategy getInnerStrategy() {
            return array2SetStrategy;
        }
    };


    private final AdjacencyListStrategy array2SetStrategy = new AdjacencyListStrategy() {
        @Override
        public AdjacencyList emptyList() {
            return new ArrayAdjacencyList(array2SetStrategy,initialArraySize);
        }

        @Override
        public int extensionThreshold() {
            return array2Set;
        }

        @Override
        public AdjacencyList upgrade(AdjacencyList old) {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public AdjacencyListStrategy getInnerStrategy() {
            throw new UnsupportedOperationException();
        }
    };

    private StandardAdjListStrategy() {}
    
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
        Preconditions.checkArgument(old==null || old.isEmpty(),"Expected empty adjacency list");
        return new ArrayAdjacencyList(array2MapStrategy,initialArraySize);
    }

    @Override
    public AdjacencyListStrategy getInnerStrategy() {
        throw new UnsupportedOperationException();
    }
    
    public static StandardAdjListStrategy getInstance() {
        return INSTANCE;
    }
}
