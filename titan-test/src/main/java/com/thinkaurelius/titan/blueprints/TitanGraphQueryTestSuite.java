package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.GraphQueryTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TitanGraphQueryTestSuite extends GraphQueryTestSuite {


    public TitanGraphQueryTestSuite(final GraphTest graphTest) {
        super(graphTest);
    }

    @Override
    public void testGraphQueryForVertices() {
        TitanGraph g = (TitanGraph) graphTest.generateGraph();
        if (g.getRelationType("age") == null) {
            PropertyKey age = g.makePropertyKey("age").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        }
        g.shutdown();
        super.testGraphQueryForVertices();
    }

    @Override
    public void testGraphQueryForEdges() {
        TitanGraph g = (TitanGraph) graphTest.generateGraph();
        if (g.getRelationType("weight") == null) {
            PropertyKey weight = g.makePropertyKey("weight").dataType(Double.class).cardinality(Cardinality.SINGLE).make();
        }
        g.shutdown();
        super.testGraphQueryForEdges();
    }
}
