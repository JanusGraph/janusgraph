package org.janusgraph.graphdb.thrift;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusOperationCountingTest;
import org.junit.BeforeClass;

public class ThriftOperationCountingTest extends JanusOperationCountingTest {

    @BeforeClass
    public static void beforeClass() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return CassandraStorageSetup.getCassandraThriftGraphConfiguration(getClass().getSimpleName());
    }

    @Override
    public boolean storeUsesConsistentKeyLocker() {
        return true;
    }

}
