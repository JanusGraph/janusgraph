package com.thinkaurelius.titan.blueprints;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TestSuite;
import org.apache.commons.configuration.Configuration;

public abstract class AbstractHazelcastBlueprintsTest extends TitanBlueprintsTest {

    private static final Set<String> EXCLUDED_METHODS = new HashSet<String>() {{
        add("testGraphDataPersists");
        add("testAutoIndexKeyManagementWithPersistence");
    }};

    private final Configuration conf;
    private StandardTitanGraph graph;

    protected AbstractHazelcastBlueprintsTest(Configuration config) {
        conf = config;
    }

    @Override
    public void testTransactionalGraphTestSuite() {
        // throws "nested transactions are not allowed error from inside of Blueprints
    }

    @Override
    public void testQueryTestSuite() throws Exception {
        // segfaults on reading from socket on Mac OS X
    }

    @Override
    public synchronized void startUp() {
        // nothing to start
    }

    @Override
    public void shutDown() {
        try {
            graph.clear();
            graph.shutdown();
        } catch (StorageException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Graph generateGraph() {
        graph = (StandardTitanGraph) TitanFactory.open(conf);
        return graph;
    }

    @Override
    public void cleanUp() throws StorageException {
    }

    @Override
    public boolean supportsMultipleGraphs() {
        return false;
    }

    @Override
    public Graph generateGraph(String s) {
        throw new UnsupportedOperationException();
    }

    public void doTestSuite(TestSuite testSuite, Set<String> ignoreTests) throws Exception {
        startUp();
        cleanUp();
        for (Method method : testSuite.getClass().getMethods()) {
            String methodName = method.getName();

            if (EXCLUDED_METHODS.contains(methodName) || ignoreTests.contains(methodName) || !methodName.startsWith("test"))
                continue;

            try {
                System.out.println("Testing " + method.getName() + "...");
                method.invoke(testSuite);
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
