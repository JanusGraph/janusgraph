package com.thinkaurelius.titan.graphdb;

import org.apache.commons.configuration.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.InfinispanStorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;

/*
 * This test assumes that graph storage is persistent, that is, that a graph survives g.shutdown() and a subsequent reopen.  This is not the case for Infinispan cache, so we override several test helper methods in the superclass to prevent graph shutdown until after all of the class's methods have executed.
 */
public abstract class AbstractInfinispanCacheGraphTest extends TitanGraphTest {
    
    public AbstractInfinispanCacheGraphTest(boolean tx) {
        super(InfinispanStorageSetup.getInfinispanCacheGraphConfig(tx));
    }
    @Override
    public void testTypes() {
        // explicitly disabling testTypes() as in InMemoryGraphTest
    }

    @Override
    public void clopen() {
        if (null != tx && tx.isOpen())
            tx.commit();
        
        newTx();
    }

}
