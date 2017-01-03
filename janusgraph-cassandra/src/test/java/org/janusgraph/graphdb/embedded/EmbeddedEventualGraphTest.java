package org.janusgraph.graphdb.embedded;

import org.junit.BeforeClass;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusEventualGraphTest;

public class EmbeddedEventualGraphTest extends JanusEventualGraphTest {

    @BeforeClass
    public static void startEmbeddedCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return CassandraStorageSetup.getEmbeddedCassandraPartitionGraphConfiguration(getClass().getSimpleName());
    }

}
