package com.thinkaurelius.titan.blueprints;

import com.tinkerpop.blueprints.BaseTest;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Graph;
import com.tinkerpop.blueprints.pgm.TestSuite;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.GraphTest;
import com.tinkerpop.blueprints.pgm.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.pgm.util.io.graphml.GraphMLReader;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerBenchmarkTestSuite extends TestSuite {

    private static final int TOTAL_RUNS = 10;

    public TinkerBenchmarkTestSuite() {

    }

    public TinkerBenchmarkTestSuite(final GraphTest graphTest) {
        super(graphTest);
    }


    public void testTinkerGraph() throws Exception {
        double totalTime = 0.0d;
        Graph graph = new TinkerGraph();
        GraphMLReader.inputGraph(graph, GraphMLReader.class.getResourceAsStream("graph-example-2.xml"));

        for (int i = 0; i < TOTAL_RUNS; i++) {
            this.stopWatch();
            int counter = 0;
            for (final Vertex vertex : graph.getVertices()) {
                counter++;
                for (final Edge edge : vertex.getOutEdges()) {
                    counter++;
                    final Vertex vertex2 = edge.getInVertex();
                    counter++;
                    for (final Edge edge2 : vertex2.getOutEdges()) {
                        counter++;
                        final Vertex vertex3 = edge2.getInVertex();
                        counter++;
                        for (final Edge edge3 : vertex3.getOutEdges()) {
                            counter++;
                            edge3.getOutVertex();
                            counter++;
                        }
                    }
                }
            }
            double currentTime = this.stopWatch();
            totalTime = totalTime + currentTime;
            BaseTest.printPerformance(graph.toString(), counter, "TinkerGraph elements touched", currentTime);
            graph.shutdown();
        }
        BaseTest.printPerformance("TinkerGraph", 1, "TinkerGraph experiment average", totalTime / (double) TOTAL_RUNS);
    }

}