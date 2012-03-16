package com.thinkaurelius.titan.blueprints;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.DiskgraphTest;
import com.tinkerpop.blueprints.pgm.*;
import com.tinkerpop.blueprints.pgm.impls.GraphTest;
import com.tinkerpop.blueprints.pgm.util.io.graphml.GraphMLReaderTestSuite;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class TitanGraphTest extends GraphTest {

    public TitanGraphTest() {
        this.allowsDuplicateEdges = true;
        this.allowsSelfLoops = true;
        this.isPersistent = true;
        this.isRDFModel = false;
        this.supportsVertexIteration = false;
        this.supportsEdgeIteration = false;
        this.supportsVertexIndex = true;
        this.supportsEdgeIndex = false;
        this.ignoresSuppliedIds = true;
        this.supportsTransactions = true;

        this.allowSerializableObjectProperty = true;
        this.allowBooleanProperty = true;
        this.allowDoubleProperty = true;
        this.allowFloatProperty = true;
        this.allowIntegerProperty = true;
        this.allowPrimitiveArrayProperty = true;
        this.allowListProperty = true;
        this.allowLongProperty = true;
        this.allowMapProperty = true;
        this.allowStringProperty = true;
        

    }

    public void testVertexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new VertexTestSuite(this),ImmutableSet.of("testVertexEquality","testNoConcurrentModificationException"));
        printTestPerformance("VertexTestSuite", this.stopWatch());
    }

    public void testEdgeTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new EdgeTestSuite(this),ImmutableSet.of("testGetEdges","testGetNonExistantEdges",
                "testNoConcurrentModificationException"));
        printTestPerformance("EdgeTestSuite", this.stopWatch());
    }

    public void testGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphTestSuite(this),ImmutableSet.of("testConcurrentModification"));
        printTestPerformance("GraphTestSuite", this.stopWatch());
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

    public void testAutomaticIndexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new AutomaticIndexTestSuite(this),ImmutableSet.of("testEdgeLabelIndexing"));
        printTestPerformance("AutomaticIndexTestSuite", this.stopWatch());
    }

    public void testTransactionalGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new TransactionalGraphTestSuite(this));
        printTestPerformance("TransactionalGraphTestSuite", this.stopWatch());
    }

    public void testGraphMLReaderTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphMLReaderTestSuite(this));
        printTestPerformance("GraphMLReaderTestSuite", this.stopWatch());
    }

    @Override
    public Graph getGraphInstance() {
        graph = new TitanGraph(DiskgraphTest.homeDir);
        graph.setMaxBufferSize(0);
        return graph;
    }
    
    private TitanGraph graph = null;

    @Override
    public void doTestSuite(TestSuite testSuite) throws Exception {    
        doTestSuite(testSuite, new HashSet<String>());
    }
    
    public void doTestSuite(TestSuite testSuite, Set<String> ignoreTests) throws Exception {
        DiskgraphTest.deleteHomeDir();
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
                if (graph!=null) {
                    graph.shutdown();
                    graph=null;
                }
                DiskgraphTest.deleteHomeDir();
            }
        }
    }

}
