package com.thinkaurelius.titan.graphdb.thrift;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import org.junit.BeforeClass;

public class ThriftGraphCacheTest extends TitanGraphTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return StorageSetup.addPermanentCache(CassandraStorageSetup.getCassandraThriftConfiguration(getClass().getSimpleName()));
    }


    @BeforeClass
    public static void beforeClass() {
        CassandraStorageSetup.startCleanEmbedded();
    }



    @Override
    protected boolean isLockingOptimistic() {
        return true;
    }
}
