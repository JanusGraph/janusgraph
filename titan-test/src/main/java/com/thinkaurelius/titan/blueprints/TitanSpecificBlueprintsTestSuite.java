package com.thinkaurelius.titan.blueprints;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.GraphTest;
import org.junit.Assert;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TitanSpecificBlueprintsTestSuite extends TestSuite {

    public TitanSpecificBlueprintsTestSuite(final GraphTest graphTest) {
        super(graphTest);
    }

    public void testVertexReattachment() {
        TransactionalGraph graph = (TransactionalGraph) graphTest.generateGraph();
        Vertex a = graph.addVertex(null);
        Vertex b = graph.addVertex(null);
        Edge e = graph.addEdge(null, a, b, "friend");
        graph.commit();

        a = graph.getVertex(a);
        Assert.assertNotNull(a);
        Assert.assertEquals(1, BaseTest.count(a.getVertices(Direction.OUT)));

        graph.shutdown();
    }

}
