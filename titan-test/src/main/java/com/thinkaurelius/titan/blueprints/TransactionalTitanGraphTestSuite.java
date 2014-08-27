package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.core.schema.TitanManagement;
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

        TitanManagement mgmt = graph.getManagementSystem();
        mgmt.makeEdgeLabel("friend").make();
        mgmt.makePropertyKey("test").dataType(Long.class).make();
        mgmt.makePropertyKey("blah").dataType(Float.class).make();
        mgmt.makePropertyKey("bloop").dataType(Integer.class).make();


        mgmt.commit();
        graph.shutdown();
        super.testCompetingThreads();
    }
}
