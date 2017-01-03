package org.janusgraph.graphdb.astyanax;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphConcurrentTest;
import org.janusgraph.testcategory.PerformanceTests;

import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@Category({PerformanceTests.class})
public class AstyanaxGraphConcurrentTest extends JanusGraphConcurrentTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }


    @Override
    public WriteConfiguration getConfiguration() {
        return CassandraStorageSetup.getAstyanaxGraphConfiguration(getClass().getSimpleName());
    }
}
