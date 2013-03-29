package com.thinkaurelius.titan.blueprints;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanBenchmarkSuite extends TestSuite {

    private static final int TOTAL_RUNS = 10;

    public TitanBenchmarkSuite() {
    }

    public TitanBenchmarkSuite(final GraphTest graphTest) {
        super(graphTest);
    }

    public void testTitanGraph() throws Exception {
        double totalTime = 0.0d;
        Graph graph = graphTest.generateGraph();
        GraphMLReader.inputGraph(graph, GraphMLReader.class.getResourceAsStream("graph-example-2.xml"));

        for (int i = 0; i < TOTAL_RUNS; i++) {
            this.stopWatch();
            int counter = 0;
            for (final Vertex vertex : graph.getVertices()) {
                counter++;
                for (final Edge edge : vertex.getEdges(Direction.OUT)) {
                    counter++;
                    final Vertex vertex2 = edge.getVertex(Direction.IN);
                    counter++;
                    for (final Edge edge2 : vertex2.getEdges(Direction.OUT)) {
                        counter++;
                        final Vertex vertex3 = edge2.getVertex(Direction.IN);
                        counter++;
                        for (final Edge edge3 : vertex3.getEdges(Direction.OUT)) {
                            counter++;
                            edge3.getVertex(Direction.OUT);
                            counter++;
                        }
                    }
                }
            }
            double currentTime = this.stopWatch();
            totalTime = totalTime + currentTime;
            BaseTest.printPerformance(graph.toString(), counter, "TinkerGraph elements touched", currentTime);
        }
        BaseTest.printPerformance("TinkerGraph", 1, "TinkerGraph experiment average", totalTime / (double) TOTAL_RUNS);
        graph.shutdown();
    }

}
