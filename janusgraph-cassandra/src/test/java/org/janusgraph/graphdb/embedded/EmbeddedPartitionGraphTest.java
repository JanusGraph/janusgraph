package org.janusgraph.graphdb.embedded;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.TitanEventualGraphTest;
import org.janusgraph.graphdb.TitanPartitionGraphTest;
import org.junit.BeforeClass;

public class EmbeddedPartitionGraphTest extends TitanPartitionGraphTest {

    @BeforeClass
    public static void startEmbeddedCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return CassandraStorageSetup.getEmbeddedCassandraPartitionGraphConfiguration(getClass().getSimpleName());
    }

}
