package com.thinkaurelius.titan.blueprints;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.GraphTest;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class TitanSpecificBlueprintsTestSuite extends TestSuite {

    public TitanSpecificBlueprintsTestSuite(final GraphTest graphTest) {
        super(graphTest);
    }
    
    public void testVertexReattachment() {
        TransactionalGraph graph = (TransactionalGraph) graphTest.generateGraph();
        Vertex a = graph.addVertex(null);
        Vertex b = graph.addVertex(null);
        Edge e = graph.addEdge(null, a, b, convertId(graph,"friend"));
        graph.stopTransaction(TransactionalGraph.Conclusion.SUCCESS);

        a  = graph.getVertex(a);
        assertNotNull(a);
        assertEquals(1,count(a.getVertices(Direction.OUT)));

        graph.shutdown();
    }
    
}
