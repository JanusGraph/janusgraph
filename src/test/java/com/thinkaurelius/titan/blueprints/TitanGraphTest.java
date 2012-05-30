package com.thinkaurelius.titan.blueprints;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.testutil.MemoryAssess;
import com.tinkerpop.blueprints.*;
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

public class TitanGraphTest extends GraphTest {

    public void testTitanBenchmarkTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new TitanBenchmarkSuite(this));
        printTestPerformance("TitanBenchmarkTestSuite", this.stopWatch());
    }

    public void testVertexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new VertexTestSuite(this));
        printTestPerformance("VertexTestSuite", this.stopWatch());
    }

    public void testEdgeTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new EdgeTestSuite(this),ImmutableSet.of("testGetEdges","testGetNonExistantEdges"));
        printTestPerformance("EdgeTestSuite", this.stopWatch());
    }

    public void testGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphTestSuite(this));
        printTestPerformance("GraphTestSuite", this.stopWatch());
    }

    public void testQueryTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new QueryTestSuite(this));
        printTestPerformance("QueryTestSuite", this.stopWatch());
    }

    public void testKeyIndexableGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new KeyIndexableGraphTestSuite(this), ImmutableSet.of("testAutoIndexKeyDroppingWithPersistence"));
        printTestPerformance("KeyIndexableGraphTestSuite", this.stopWatch());
    }

    public void testTransactionalGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new TransactionalGraphTestSuite(this), ImmutableSet.of("testTransactionsForEdges"));
        printTestPerformance("TransactionalTitanGraphTestSuite", this.stopWatch());
    }

    public void testGraphMLReaderTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphMLReaderTestSuite(this));
        printTestPerformance("GraphMLReaderTestSuite", this.stopWatch());
    }

    public void testGraphSONReaderTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphSONReaderTestSuite(this));
        printTestPerformance("GraphSONReaderTestSuite", this.stopWatch());
    }

    public void testGMLReaderTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GMLReaderTestSuite(this));
        printTestPerformance("GMLReaderTestSuite", this.stopWatch());
    }

    @Override
    public Graph generateGraph() {
        graph = TitanFactory.open(StorageSetup.getHomeDir());
        return graph;
    }

    private TitanGraph graph = null;

    @Override
    public void doTestSuite(TestSuite testSuite) throws Exception {    
        doTestSuite(testSuite, new HashSet<String>());
    }
    
    public void doTestSuite(TestSuite testSuite, Set<String> ignoreTests) throws Exception {
        StorageSetup.deleteHomeDir();
        for (Method method : testSuite.getClass().getDeclaredMethods()) {
            if (ignoreTests.contains(method.getName())
                    || !method.getName().startsWith("test")) continue;
            try {

                System.out.println("Testing " + method.getName() + "...");
                method.invoke(testSuite);
//                System.out.println("##################### MEMORY ############");
//                System.out.println(MemoryAssess.getMemoryUse()/1024);
                graph = null;
            } catch (Throwable e) {
                System.err.println("Encountered error in " + method.getName());
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                if (graph!=null && graph.isOpen()) {
                    graph.shutdown();
                    graph=null;
                }
                StorageSetup.deleteHomeDir();
            }
        }
    }

}
