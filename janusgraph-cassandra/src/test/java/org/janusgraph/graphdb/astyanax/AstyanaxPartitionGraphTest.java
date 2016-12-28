package org.janusgraph.graphdb.astyanax;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.TitanGraphTest;
import org.janusgraph.graphdb.TitanPartitionGraphTest;
import org.junit.BeforeClass;

public class AstyanaxPartitionGraphTest extends TitanPartitionGraphTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return CassandraStorageSetup.getAstyanaxGraphConfiguration(getClass().getSimpleName());
    }

}
