package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.tinkerpop.blueprints.Direction;
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
        if (g.getType("age") == null) {
            TitanKey age = g.makeKey("age").dataType(Integer.class).single().make();
        }
        g.shutdown();
        super.testGraphQueryForVertices();
    }

    @Override
    public void testGraphQueryForEdges() {
        TitanGraph g = (TitanGraph) graphTest.generateGraph();
        if (g.getType("weight") == null) {
            TitanKey weight = g.makeKey("weight").dataType(Double.class).single().make();
        }
        g.shutdown();
        super.testGraphQueryForEdges();
    }
}
