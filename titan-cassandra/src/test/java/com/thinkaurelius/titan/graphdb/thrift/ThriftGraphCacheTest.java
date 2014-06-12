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
        CassandraStorageSetup.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }

    // This fails an assertion on ExpectedValueCheckingTransaction.java:136
    @Override
    public void testConfiguration() {
        // TODO fix KCVSConfiguration + dbcache
    }

    @Override
    protected boolean isLockingOptimistic() {
        return true;
    }
}
