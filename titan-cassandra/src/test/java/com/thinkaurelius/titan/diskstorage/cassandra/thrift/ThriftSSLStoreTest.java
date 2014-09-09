package com.thinkaurelius.titan.diskstorage.cassandra.thrift;


import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.testcategory.CassandraSSLTests;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@Category({ CassandraSSLTests.class })
public class ThriftSSLStoreTest extends ThriftStoreTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    public ModifiableConfiguration getBaseStorageConfiguration() {
        return CassandraStorageSetup.getCassandraThriftSSLConfiguration(this.getClass().getSimpleName());
    }
}
