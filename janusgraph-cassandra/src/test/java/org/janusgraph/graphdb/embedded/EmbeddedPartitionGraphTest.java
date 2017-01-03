package org.janusgraph.graphdb.embedded;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusEventualGraphTest;
import org.janusgraph.graphdb.JanusPartitionGraphTest;
import org.junit.BeforeClass;

public class EmbeddedPartitionGraphTest extends JanusPartitionGraphTest {

    @BeforeClass
    public static void startEmbeddedCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return CassandraStorageSetup.getEmbeddedCassandraPartitionGraphConfiguration(getClass().getSimpleName());
    }

}
