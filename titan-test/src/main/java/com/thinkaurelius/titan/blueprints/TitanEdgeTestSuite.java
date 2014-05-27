package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.graphdb.blueprints.TitanBlueprintsGraph;
import com.tinkerpop.blueprints.EdgeTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;


public class TitanEdgeTestSuite extends EdgeTestSuite {

    public TitanEdgeTestSuite(final GraphTest graphTest) {
        super(graphTest);
    }

}
