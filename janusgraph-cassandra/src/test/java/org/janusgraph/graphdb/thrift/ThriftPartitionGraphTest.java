package org.janusgraph.graphdb.thrift;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusOperationCountingTest;
import org.janusgraph.graphdb.JanusPartitionGraphTest;
import org.junit.BeforeClass;

public class ThriftPartitionGraphTest extends JanusPartitionGraphTest {

    @BeforeClass
    public static void beforeClass() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return CassandraStorageSetup.getCassandraThriftGraphConfiguration(getClass().getSimpleName());
    }

}
