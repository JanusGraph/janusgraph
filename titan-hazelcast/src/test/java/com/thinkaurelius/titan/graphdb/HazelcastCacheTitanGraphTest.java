package com.thinkaurelius.titan.graphdb;

import com.thinkaurelius.titan.HazelcastStorageSetup;
import com.thinkaurelius.titan.diskstorage.StorageException;

public class HazelcastCacheTitanGraphTest extends TitanGraphTest {

    public HazelcastCacheTitanGraphTest() {
        super(HazelcastStorageSetup.getHazelcastCacheGraphConfig(false));
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

        graph.clear();
    }

    @Override
    public void clopen() {
        newTx();
    }

    @Override
    public void close() {
    }
}