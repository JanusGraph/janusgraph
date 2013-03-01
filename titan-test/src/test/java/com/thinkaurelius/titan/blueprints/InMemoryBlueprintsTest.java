package com.thinkaurelius.titan.blueprints;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.BaseTest;
import com.tinkerpop.blueprints.EdgeTestSuite;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphTestSuite;
import com.tinkerpop.blueprints.KeyIndexableGraphTestSuite;
import com.tinkerpop.blueprints.QueryTestSuite;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.VertexTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.gml.GMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReaderTestSuite;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public abstract class InMemoryBlueprintsTest extends GraphTest {

    /*public void testTitanBenchmarkTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new TitanBenchmarkSuite(this));
        printTestPerformance("TitanBenchmarkTestSuite", this.stopWatch());
    }*/

    public void testVertexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new VertexTestSuite(this));
        BaseTest.printTestPerformance("VertexTestSuite", this.stopWatch());
    }

    public void testEdgeTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new EdgeTestSuite(this), ImmutableSet.of("testGetEdges", "testGetNonExistantEdges"));
        BaseTest.printTestPerformance("EdgeTestSuite", this.stopWatch());
    }

    public void testGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphTestSuite(this));
        BaseTest.printTestPerformance("GraphTestSuite", this.stopWatch());
    }

    public void testQueryTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new QueryTestSuite(this));
        BaseTest.printTestPerformance("QueryTestSuite", this.stopWatch());
    }

    //Titan does not support manual indexes

//    public void testIndexableGraphTestSuite() throws Exception {
//        this.stopWatch();
//        doTestSuite(new IndexableGraphTestSuite(this));
//        printTestPerformance("IndexableGraphTestSuite", this.stopWatch());
//    }


//    public void testIndexTestSuite() throws Exception {
//        this.stopWatch();
//        doTestSuite(new IndexTestSuite(this));
//        printTestPerformance("IndexTestSuite", this.stopWatch());
//    }

    public void testKeyIndexableGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new KeyIndexableGraphTestSuite(this), ImmutableSet.of("testReIndexingOfElements", "testGettingVerticesAndEdgesWithKeyValue"));
        BaseTest.printTestPerformance("KeyIndexableGraphTestSuite", this.stopWatch());
    }

    public void testGraphMLReaderTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphMLReaderTestSuite(this));
        BaseTest.printTestPerformance("GraphMLReaderTestSuite", this.stopWatch());
    }

   /*
   TODO: MAKE ALIVE!
   public void testGraphSONReaderTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphSONReaderTestSuite(this));
        BaseTest.printTestPerformance("GraphSONReaderTestSuite", this.stopWatch());
    }
    */

    public void testGMLReaderTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GMLReaderTestSuite(this));
        BaseTest.printTestPerformance("GMLReaderTestSuite", this.stopWatch());
    }

    @Override
    public Graph generateGraph() {
        graph = StorageSetup.getInMemoryGraph();
        return graph;
    }

    @Override
    public Graph generateGraph(String graphDirectoryName) {
        throw new UnsupportedOperationException();
    }

    private TitanGraph graph = null;

    @Override
    public void doTestSuite(TestSuite testSuite) throws Exception {
        doTestSuite(testSuite, new HashSet<String>());
    }

    public void doTestSuite(TestSuite testSuite, Set<String> ignoreTests) throws Exception {
        StorageSetup.deleteHomeDir();
        for (Method method : testSuite.getClass().getDeclaredMethods()) {
            if (ignoreTests.contains(method.getName())) continue;
            try {
                if (method.getName().startsWith("test")) {
                    System.out.println("Testing " + method.getName() + "...");
                    method.invoke(testSuite);
                }
                graph = null;
            } catch (Throwable e) {
                System.err.println("Encountered error in " + method.getName());
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                if (graph != null) {
                    graph.shutdown();
                    graph = null;
                }
                StorageSetup.deleteHomeDir();
            }
        }
    }

}
