package com.thinkaurelius.titan.blueprints;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.gml.GMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReaderTestSuite;

import java.io.File;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
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

    protected final Map<String, TitanGraph> openGraphs = new HashMap<String, TitanGraph>();



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
        doTestSuite(new TitanEdgeTestSuite(this), ImmutableSet.of("testEdgeIterator")); //had to be removed due to a bug in the test TODO: re-add with next blueprints release
        printTestPerformance("EdgeTestSuite", this.stopWatch());
    }

    public void testGraphTestSuite() throws Exception {
        this.stopWatch();
         //Excluded "testStringRepresentation" test case because toString representation is non-standard
        //Excluded "testConnectivityPatterns","testTreeConnectivity","testGraphDataPersists" because of bug in test. TODO: re-add with next blueprints release
        doTestSuite(new GraphTestSuite(this), ImmutableSet.of("testStringRepresentation",
                "testConnectivityPatterns","testTreeConnectivity","testGraphDataPersists"));
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

    //public abstract void cleanUp() throws BackendException;

    public abstract void beforeSuite();

    public void afterSuite() {
        // Most impls do nothing by default
    }
    protected String getMostRecentMethodName() {
        return lastSeenMethodName;
    }

    @Override
    public Object convertId(final Object id) {
        return null;
    }

    @Override
    public String convertLabel(String label) {
        return label;
    }

    @Override
    public void doTestSuite(TestSuite testSuite) throws Exception {
        doTestSuite(testSuite, new HashSet<String>());
    }

    protected abstract TitanGraph openGraph(String uid);

    protected void beforeOpeningGraph(String uid) {
        log.debug("Opening graph[uid={}] for the first time", uid);
    }

    protected void extraCleanUp(String uid) throws BackendException {

    }

    @Override
    public Graph generateGraph() {
        return generateGraph("_DEFAULT_TITAN_GRAPH_UID");
    }

    @Override
    public Graph generateGraph(String uid) {
        synchronized (openGraphs) {
            if (!openGraphs.containsKey(uid)) {
                beforeOpeningGraph(uid);
            } else if (openGraphs.get(uid).isOpen()) {
                log.warn("Detected possible graph[uid={}] leak in Blueprints GraphTest method {}, shutting down the leaked graph",
                        uid, getMostRecentMethodName());
                openGraphs.get(uid).shutdown();
            } else {
                log.debug("Reopening previously-closed graph[uid={}]", uid);
            }
        }
        log.info("Opening graph with uid={}", uid);
        openGraphs.put(uid, openGraph(uid));
        return openGraphs.get(uid);
    }

    public void cleanUp() throws BackendException {
        synchronized (openGraphs) {
            for (Map.Entry<String, TitanGraph> entry : openGraphs.entrySet()) {
                String uid = entry.getKey();
                TitanGraph g = entry.getValue();
                if (g.isOpen()) {
                    log.warn("Detected possible graph[uid={}] leak in Blueprints GraphTest method {}, shutting down the leaked graph",
                        uid, getMostRecentMethodName());
                    g.shutdown();
                }
                extraCleanUp(uid);
            }
            openGraphs.clear();
        }
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
