package org.janusgraph.graphdb.embedded;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphEventualGraphTest;
import org.janusgraph.graphdb.JanusGraphPartitionGraphTest;
import org.junit.BeforeClass;

public class EmbeddedPartitionGraphTest extends JanusGraphPartitionGraphTest {

    @BeforeClass
    public static void startEmbeddedCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return CassandraStorageSetup.getEmbeddedCassandraPartitionGraphConfiguration(getClass().getSimpleName());
    }

}
