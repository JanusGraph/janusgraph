package org.janusgraph.diskstorage.cassandra.thrift;


import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.testcategory.CassandraSSLTests;
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
