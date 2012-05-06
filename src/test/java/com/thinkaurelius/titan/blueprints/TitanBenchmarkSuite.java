package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.core.Direction;
import com.thinkaurelius.titan.core.GraphTransaction;
import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.core.Relationship;
import com.tinkerpop.blueprints.BaseTest;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Graph;
import com.tinkerpop.blueprints.pgm.TestSuite;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.GraphTest;
import com.tinkerpop.blueprints.pgm.util.io.graphml.GraphMLReader;

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
        Graph graph = graphTest.getGraphInstance();
        GraphMLReader.inputGraph(graph, GraphMLReader.class.getResourceAsStream("graph-example-2.xml"));
        //graph.shutdown();

        for (int i = 0; i < TOTAL_RUNS; i++) {
            //graph = graphTest.getGraphInstance();
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
            BaseTest.printPerformance(graph.toString(), counter, "TitanGraph elements touched", currentTime);
//            graph.shutdown();
        }
        BaseTest.printPerformance("TitanGraph", 1, "TitanGraph experiment average", totalTime / (double) TOTAL_RUNS);
        graph.shutdown();
    }



    public void testTitanGraphNative() throws Exception {
        double totalTime = 0.0d;
        TitanGraph graph = (TitanGraph) graphTest.getGraphInstance();
        GraphMLReader.inputGraph(graph, GraphMLReader.class.getResourceAsStream("graph-example-2.xml"));
        //graph.shutdown();
        GraphTransaction tx = graph.getNativeTransaction();
        
        for (int i = 0; i < TOTAL_RUNS; i++) {
            //graph = graphTest.getGraphInstance();
            this.stopWatch();
            int counter = 0;
            for (final Node vertex : tx.getAllNodes()) {
                counter++;
                for (final Relationship edge : vertex.getRelationships(Direction.Out)) {
                    counter++;
                    final Node vertex2 = edge.getEnd();
                    counter++;
                    for (final Relationship edge2 : vertex2.getRelationships(Direction.Out)) {
                        counter++;
                        final Node vertex3 = edge2.getEnd();
                        counter++;
                        for (final Relationship edge3 : vertex3.getRelationships(Direction.Out)) {
                            counter++;
                            edge3.getEnd();
                            counter++;
                        }
                    }
                }
            }
            double currentTime = this.stopWatch();
            totalTime = totalTime + currentTime;
            BaseTest.printPerformance(graph.toString(), counter, "TitanGraphNative elements touched", currentTime);
//            graph.shutdown();
        }
        BaseTest.printPerformance("TitanGraphNative", 1, "TitanGraphNative experiment average", totalTime / (double) TOTAL_RUNS);
        graph.shutdown();
    }


}
