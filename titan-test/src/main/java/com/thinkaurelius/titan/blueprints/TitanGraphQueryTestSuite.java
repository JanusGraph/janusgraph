package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;
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
            TitanManagement mgmt = g.getManagementSystem();
            mgmt.makePropertyKey("age").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
            mgmt.commit();
        }
        g.shutdown();
        super.testGraphQueryForVertices();
    }

    @Override
    public void testGraphQueryForEdges() {
        TitanGraph g = (TitanGraph) graphTest.generateGraph();
        if (g.getRelationType("weight") == null) {
            TitanManagement mgmt = g.getManagementSystem();
            mgmt.makePropertyKey("weight").dataType(Double.class).cardinality(Cardinality.SINGLE).make();
            mgmt.commit();
        }
        g.shutdown();
        super.testGraphQueryForEdges();
    }
}
