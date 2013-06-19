package com.thinkaurelius.titan.blueprints;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.gml.GMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReaderTestSuite;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

// import com.thinkaurelius.titan.diskstorage.berkeleyje.BerkeleyJEStoreManager;

/**
 * @author Matthias Broecheler (http://www.matthiasb.com)
 */

public abstract class TitanBlueprintsTest extends GraphTest {

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
        this.stopWatch();
        doTestSuite(new EdgeTestSuite(this));
        printTestPerformance("EdgeTestSuite", this.stopWatch());
    }

    public void testGraphTestSuite() throws Exception {
        this.stopWatch();                       //Excluded test case because toString representation is non-standard
        doTestSuite(new GraphTestSuite(this), ImmutableSet.of("testStringRepresentation","testDataTypeValidationOnProperties"));
        printTestPerformance("GraphTestSuite", this.stopWatch());
    }

    public void testQueryTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new TitanGraphQueryTestSuite(this));
        printTestPerformance("GraphQueryTestSuite", this.stopWatch());
    }
    
    public void testKeyIndexableGraphTestSuite() throws Exception {
        this.stopWatch();                                   //Excluded test cases because Titan does not yet support dropping or modifying key indexes
        doTestSuite(new KeyIndexableGraphTestSuite(this), ImmutableSet.of("testAutoIndexKeyDroppingWithPersistence", "testReIndexingOfElements"));
        printTestPerformance("KeyIndexableGraphTestSuite", this.stopWatch());
    }

    public void testTransactionalGraphTestSuite() throws Exception {
        this.stopWatch();             
        Set<String> excludedTests = new HashSet<String>();
        if (!supportsMultipleGraphs()) excludedTests.add("testCompetingThreadsOnMultipleDbInstances");
        doTestSuite(new TransactionalTitanGraphTestSuite(this), excludedTests);
        printTestPerformance("TransactionalTitanGraphTestSuite", this.stopWatch());
    }

    public void testTitanSpecificTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new TitanSpecificBlueprintsTestSuite(this));
        printTestPerformance("TitanSpecificBlueprintsTestSuite", this.stopWatch());
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

    /**
     *
     * @return true, if {@link #generateGraph(String)} is supported
     */
    public abstract boolean supportsMultipleGraphs();

    public abstract void cleanUp() throws StorageException;

    public abstract void startUp();

    public abstract void shutDown();

    @Override
    public void doTestSuite(TestSuite testSuite) throws Exception {
        doTestSuite(testSuite, new HashSet<String>());
    }

    public void doTestSuite(TestSuite testSuite, Set<String> ignoreTests) throws Exception {
        startUp();
        cleanUp();
        for (Method method : testSuite.getClass().getMethods()) {
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
