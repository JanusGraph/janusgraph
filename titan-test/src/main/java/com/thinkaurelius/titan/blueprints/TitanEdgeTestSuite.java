package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.graphdb.blueprints.TitanBlueprintsGraph;
import com.tinkerpop.blueprints.EdgeTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;


public class TitanEdgeTestSuite extends EdgeTestSuite {

    public TitanEdgeTestSuite(final GraphTest graphTest) {
        super(graphTest);
    }

    @Override
    public void testGetEdgesByLabel() {

        /*
         * I thought this had a faint shot of resolving the test failure in
         * testGetEdgesByLabel(), but it seems to have no effect. I still see a
         * NumberFormatException when executing graph.query().has("label",
         * "test1").edges().
         */
        TitanBlueprintsGraph graph = (TitanBlueprintsGraph) graphTest.generateGraph();
        graph.makeLabel("test1").make();
        graph.makeLabel("test2").make();
        graph.makeLabel("test3").make();
        graph.commit();
        graph.shutdown();

        super.testGetEdgesByLabel();
    }
}
