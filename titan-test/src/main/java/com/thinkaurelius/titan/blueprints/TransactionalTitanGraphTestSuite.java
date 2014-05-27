package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.graphdb.blueprints.TitanBlueprintsGraph;
import com.tinkerpop.blueprints.TransactionalGraphTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TransactionalTitanGraphTestSuite extends TransactionalGraphTestSuite {


    public TransactionalTitanGraphTestSuite(final GraphTest graphTest) {
        super(graphTest);
    }

    @Override
    public void testCompetingThreads() {
        TitanBlueprintsGraph graph = (TitanBlueprintsGraph) graphTest.generateGraph();
        //Need to define types before hand to avoid deadlock in transactions

        graph.makeEdgeLabel("friend").make();
        graph.makePropertyKey("test").dataType(Long.class).make();
        graph.makePropertyKey("blah").dataType(Float.class).make();
        graph.makePropertyKey("bloop").dataType(Integer.class).make();


        graph.commit();
        graph.shutdown();
        super.testCompetingThreads();
    }

}
