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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Blueprints test suite adapted for Titan. The following test suites are not supported:
 * <ul>
 *     <li>IndexableGraphTestSuite</li>
 *     <li>IndexTestSuite</li>
 *     <li>KeyIndexableGraphTestSuite</li>
 * </ul>
 * since Titan handles indexing through the schema which isn't supported in Blueprints.
 *
 * @author Matthias Broecheler (http://www.matthiasb.com)
 */
public abstract class TitanBlueprintsTest extends GraphTest {

    private static final Logger log =
            LoggerFactory.getLogger(TitanBlueprintsTest.class);

    private volatile String lastSeenMethodName;



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

    public void testVertexQueryTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new VertexQueryTestSuite(this));
        printTestPerformance("VertexQueryTestSuite", this.stopWatch());
    }

    public void testEdgeTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new TitanEdgeTestSuite(this));
        printTestPerformance("EdgeTestSuite", this.stopWatch());
    }

    public void testGraphTestSuite() throws Exception {
        this.stopWatch();                       //Excluded test case because toString representation is non-standard
        doTestSuite(new GraphTestSuite(this), ImmutableSet.of("testStringRepresentation"));
        printTestPerformance("GraphTestSuite", this.stopWatch());
    }

    public void testGraphQueryTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new TitanGraphQueryTestSuite(this));
        printTestPerformance("GraphQueryTestSuite", this.stopWatch());
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

    public abstract void beforeSuite();

    public void afterSuite() {
        // Most impls do nothing by default
    }
    protected String getMostRecentMethodName() {
        return lastSeenMethodName;
    }

    @Override
    public void doTestSuite(TestSuite testSuite) throws Exception {
        doTestSuite(testSuite, new HashSet<String>());
    }

    public void doTestSuite(TestSuite testSuite, Set<String> ignoreTests) throws Exception {
        beforeSuite();
        cleanUp();
        for (Method method : testSuite.getClass().getMethods()) {
            if (ignoreTests.contains(method.getName())
                    || !method.getName().startsWith("test")) continue;
            String name = testSuite.getClass().getSimpleName() + "." + method.getName();
            lastSeenMethodName = name;
            try {

                log.info("Testing " + name + "...");
                method.invoke(testSuite);
//                System.out.println("##################### MEMORY ############");
//                System.out.println(MemoryAssess.getMemoryUse()/1024);
//                graph = null;
            } catch (Throwable e) {
                log.error("Encountered error in " + name);
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                cleanUp();
            }
        }
        afterSuite();
    }


}
