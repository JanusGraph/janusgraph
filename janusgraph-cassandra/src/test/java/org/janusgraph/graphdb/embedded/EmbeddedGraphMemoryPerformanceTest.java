package org.janusgraph.graphdb.embedded;

import org.junit.BeforeClass;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphPerformanceMemoryTest;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class EmbeddedGraphMemoryPerformanceTest extends JanusGraphPerformanceMemoryTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return CassandraStorageSetup.getEmbeddedCassandraPartitionGraphConfiguration(getClass().getSimpleName());
    }

}
