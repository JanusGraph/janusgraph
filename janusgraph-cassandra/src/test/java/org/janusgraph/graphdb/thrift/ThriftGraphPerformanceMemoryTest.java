package org.janusgraph.graphdb.thrift;

import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.BeforeClass;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.graphdb.JanusGraphPerformanceMemoryTest;

public class ThriftGraphPerformanceMemoryTest extends JanusGraphPerformanceMemoryTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return CassandraStorageSetup.getCassandraThriftGraphConfiguration(getClass().getSimpleName());
    }


    @BeforeClass
    public static void beforeClass() {
        CassandraStorageSetup.startCleanEmbedded();
    }
}
