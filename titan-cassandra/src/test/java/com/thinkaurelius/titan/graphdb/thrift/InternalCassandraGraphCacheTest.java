package com.thinkaurelius.titan.graphdb.thrift;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import org.junit.BeforeClass;

public class InternalCassandraGraphCacheTest extends TitanGraphTest {

    public InternalCassandraGraphCacheTest() {
        super(StorageSetup.addPermanentCache(CassandraStorageSetup.getCassandraThriftGraphConfiguration(
                InternalCassandraGraphCacheTest.class.getSimpleName())));
    }

    @BeforeClass
    public static void beforeClass() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }
}
