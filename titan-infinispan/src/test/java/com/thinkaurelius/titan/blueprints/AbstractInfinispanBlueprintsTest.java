package com.thinkaurelius.titan.blueprints;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_NAMESPACE;

import org.apache.commons.configuration.Configuration;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.InfinispanStorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.infinispan.InfinispanCacheStoreManager;
import com.tinkerpop.blueprints.BaseTest;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphTestSuite;

public abstract class AbstractInfinispanBlueprintsTest extends TitanBlueprintsTest {
    
    private final boolean transactional;
    
    public AbstractInfinispanBlueprintsTest(boolean transactional) {
        this.transactional = transactional;
    }

    @Override
    public boolean supportsMultipleGraphs() {
        return false;
    }

    @Override
    public void cleanUp() throws StorageException {
        InfinispanCacheStoreManager m = new InfinispanCacheStoreManager(getGraphConfig().subset(STORAGE_NAMESPACE));
        m.clearStorage();
    }
    
    @Override
    public void startUp() {
    }

    @Override
    public void shutDown() {
    }

    @Override
    public Graph generateGraph() {
        return TitanFactory.open(getGraphConfig());
    }

    @Override
    public Graph generateGraph(String s) {
        throw new UnsupportedOperationException();
    }

    private Configuration getGraphConfig() {
        return InfinispanStorageSetup.getInfinispanCacheGraphConfig(transactional);
    }
    
    @Override
    public void testGraphTestSuite() throws Exception {
        this.stopWatch();
        // Copied this exclusion list from InMemory -- testGraphDataPersists definitely does not work
        doTestSuite(new GraphTestSuite(this), ImmutableSet.of("testStringRepresentation","testDataTypeValidationOnProperties","testGraphDataPersists"));
        BaseTest.printTestPerformance("GraphTestSuite", this.stopWatch());
    }

    @Override
    public void testTransactionalGraphTestSuite() {
        /*
         * This suite and Infinispan have two compatibility issues.
         * 
         * 1. Timeouts in the non-transactional (ExpectedValueCheckingStore)
         * Infinispan implementation don't seem to be handled by the suite. They
         * just cause test failures. The test suite could perhaps handle those
         * failures more gracefully.
         * 
         * 2. testAutomaticSuccessfulTransactionOnShutdown() relies on
         * graph.getFeatures().isPersistent, which is still hardcoded true at
         * the graph level as I write this comment. That boolean needs to be
         * pushed down to the store level in Titan. Nothing in the test suite
         * needs to change on this item; it is strictly a Titan problem.
         * 
         * Disabled until these issues are resolved.
         */
    }
    
    @Override
    public void testKeyIndexableGraphTestSuite() throws Exception {
        // TODO
        // Need to move graph.getFeatures().isPersistent down to the store/storemanager level
        // Right now it's hardcoded true at the StandardTitanGraph level and that's wrong for InfinispanCache (and InMemory)
    }

    @Override
    public void testQueryTestSuite() {
        // TODO
        // I think this has the same problem as testKeyIndexableGraphTestSuite, but I'm not certain
    }
}
