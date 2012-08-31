package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.graphdb.blueprints.TitanBlueprintsGraph;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.GraphTest;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class TransactionalTitanGraphTestSuite extends TransactionalGraphTestSuite {


    public TransactionalTitanGraphTestSuite(final GraphTest graphTest) {
        super(graphTest);
    }

    @Override
    public void testCompetingThreads() {
        TitanBlueprintsGraph graph = (TitanBlueprintsGraph) graphTest.generateGraph();
        //Need to define types before hand to avoid deadlock in transactions
        
        graph.makeType().name("friend").makeEdgeLabel();
        graph.makeType().name("test").dataType(Long.class).makePropertyKey();
        graph.makeType().name("blah").dataType(Float.class).makePropertyKey();
        graph.makeType().name("bloop").dataType(Integer.class).makePropertyKey();

        
        graph.stopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        graph.shutdown();
        super.testCompetingThreads();
    }

}
