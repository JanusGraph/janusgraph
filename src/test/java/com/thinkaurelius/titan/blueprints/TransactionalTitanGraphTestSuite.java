package com.thinkaurelius.titan.blueprints;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.GraphTest;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class TransactionalTitanGraphTestSuite extends TestSuite {


    public TransactionalTitanGraphTestSuite(final GraphTest graphTest) {
        super(graphTest);
    }

    public void testCompetingThreads2() {
        TransactionalGraph graph = (TransactionalGraph) graphTest.generateGraph();
        graph.startTransaction();
        Vertex a = graph.addVertex(null);
        Vertex b = graph.addVertex(null);
        Edge e = graph.addEdge(null, a, b, convertId(graph,"friend"));

        a.setProperty("test", 5);
        b.setProperty("blah", 0.5f);
        e.setProperty("bloop", 10);

        graph.stopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        graph.shutdown();
        competingThreads();
    }

    private void competingThreads() {
        final TransactionalGraph graph = (TransactionalGraph) graphTest.generateGraph();
        int totalThreads = 250;
        final AtomicInteger vertices = new AtomicInteger(0);
        final AtomicInteger edges = new AtomicInteger(0);
        final AtomicInteger completedThreads = new AtomicInteger(0);
        for (int i = 0; i < totalThreads; i++) {
            new Thread() {
                public void run() {
                    try {
                        Random random = new Random();
                        if (random.nextBoolean()) {
                            Vertex a = graph.addVertex(null);
                            Vertex b = graph.addVertex(null);
                            Edge e = graph.addEdge(null, a, b, convertId(graph,"friend"));

                            a.setProperty("test", this.getId());
                            b.setProperty("blah", random.nextFloat());
                            e.setProperty("bloop", random.nextInt());

                            vertices.getAndAdd(2);
                            edges.getAndAdd(1);

                        } else {
                            graph.startTransaction();
                            Vertex a = graph.addVertex(null);
                            Vertex b = graph.addVertex(null);
                            Edge e = graph.addEdge(null, a, b, convertId(graph,"friend"));
                            a.setProperty("test", this.getId());
                            b.setProperty("blah", random.nextFloat());
                            e.setProperty("bloop", random.nextInt());
                            if (random.nextBoolean()) {
                                graph.stopTransaction(TransactionalGraph.Conclusion.SUCCESS);
                                vertices.getAndAdd(2);
                                edges.getAndAdd(1);
                            } else {
                                graph.stopTransaction(TransactionalGraph.Conclusion.FAILURE);
                            }
                        }
                    } catch (Throwable e) {
                        e.printStackTrace();
                        assertTrue(false);
                    }
                    completedThreads.getAndAdd(1);
                }
            }.start();
        }

        while (completedThreads.get() < totalThreads) {
        }
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        graph.shutdown();
    }
    
}
