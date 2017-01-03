package org.janusgraph.graphdb.thrift;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusEventualGraphTest;
import org.janusgraph.graphdb.JanusGraphTest;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

public class ThriftEventualGraphTest extends JanusEventualGraphTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return CassandraStorageSetup.getCassandraThriftGraphConfiguration(getClass().getSimpleName());
    }

    @BeforeClass
    public static void beforeClass() {
        CassandraStorageSetup.startCleanEmbedded();
    }
}
