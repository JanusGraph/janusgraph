package com.thinkaurelius.titan.graphdb;

import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.InfinispanStorageSetup;
import com.thinkaurelius.titan.diskstorage.StorageException;

public class InfinispanCacheTitanGraphTest extends TitanGraphTest {

    public InfinispanCacheTitanGraphTest() {
        super(InfinispanStorageSetup.getInfinispanCacheGraphConfig(false));
    }

    @Override
    public void setUp() throws StorageException {
        if (graph == null) {
            open();
        } else {
            newTx();
        }
    }

    @Override
    public void testTypes() {
        // explicitly disabling testTypes() as in InMemoryGraphTest
    }

    @Override
    public void tearDown() throws Exception {
        if (tx != null && tx.isOpen())
            tx.commit();

        graph.shutdown();
        graph = null;
    }

    @Override
    public void clopen() {
        newTx();
    }

    @Override
    public void close() {
    }
}
