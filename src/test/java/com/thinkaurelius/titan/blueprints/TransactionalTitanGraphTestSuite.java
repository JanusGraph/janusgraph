package com.thinkaurelius.titan.blueprints;

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
        TransactionalGraph graph = (TransactionalGraph) graphTest.generateGraph();
        Vertex a = graph.addVertex(null);
        Vertex b = graph.addVertex(null);
        Edge e = graph.addEdge(null, a, b, convertId(graph,"friend"));

        a.setProperty("test", 5);
        b.setProperty("blah", 0.5f);
        e.setProperty("bloop", 10);

        graph.stopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        graph.shutdown();
        super.testCompetingThreads();
    }

}
