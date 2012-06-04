package com.thinkaurelius.titan.blueprints;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.berkeleydb.je.BerkeleyJEHelper;
import com.thinkaurelius.titan.testutil.MemoryAssess;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.gml.GMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReaderTestSuite;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * @author Matthias Broecheler (http://www.matthiasb.com)
 */

public class LocalBlueprintsTest extends GraphTest {

    /*public void testTitanBenchmarkTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new TitanBenchmarkSuite(this));
        printTestPerformance("TitanBenchmarkTestSuite", this.stopWatch());
    }*/

    public void testVertexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new VertexTestSuite(this));
        printTestPerformance("VertexTestSuite", this.stopWatch());
    }

    public void testEdgeTestSuite() throws Exception {
        this.stopWatch();                   //Exclusions: retrieval by edge id is not supported
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
        this.stopWatch();                                   //Exclusions: 1st because unsupported, 2nd for old test, 3rd does not close database
        doTestSuite(new KeyIndexableGraphTestSuite(this), ImmutableSet.of("testAutoIndexKeyDroppingWithPersistence","testAutoIndexKeyManagementWithPersistence","testReIndexingOfElements"));
        printTestPerformance("KeyIndexableGraphTestSuite", this.stopWatch());
    }

    /*public void testTransactionalGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new TransactionalGraphTestSuite(this), ImmutableSet.of("testTransactionsForEdges"));
        printTestPerformance("TransactionalTitanGraphTestSuite", this.stopWatch());
    }*/

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
        Graph graph = TitanFactory.open(StorageSetup.getHomeDir());
        return graph;
    }

    @Override
    public void doTestSuite(TestSuite testSuite) throws Exception {    
        doTestSuite(testSuite, new HashSet<String>());
    }

    public void cleanUp() {
        BerkeleyJEHelper.clearEnvironment(StorageSetup.getHomeDirFile());
        assertFalse(StorageSetup.getHomeDirFile().exists() && StorageSetup.getHomeDirFile().listFiles().length>0);
    }

    public void startUp() {
        //Nothing
    }

    public void shutDown() {
        assertFalse(StorageSetup.getHomeDirFile().exists() && StorageSetup.getHomeDirFile().listFiles().length>0);
    }

    public void doTestSuite(TestSuite testSuite, Set<String> ignoreTests) throws Exception {
        startUp();
        cleanUp();
        for (Method method : testSuite.getClass().getDeclaredMethods()) {
            if (ignoreTests.contains(method.getName())
                    || !method.getName().startsWith("test")) continue;
            try {

                System.out.println("Testing " + method.getName() + "...");
                method.invoke(testSuite);
//                System.out.println("##################### MEMORY ############");
//                System.out.println(MemoryAssess.getMemoryUse()/1024);
//                graph = null;
            } catch (Throwable e) {
                System.err.println("Encountered error in " + method.getName());
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                cleanUp();
            }
        }
        shutDown();
    }


}
