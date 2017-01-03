package org.janusgraph.graphdb.astyanax;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphTest;
import org.janusgraph.graphdb.JanusPartitionGraphTest;
import org.junit.BeforeClass;

public class AstyanaxPartitionGraphTest extends JanusPartitionGraphTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return CassandraStorageSetup.getAstyanaxGraphConfiguration(getClass().getSimpleName());
    }

}
