package org.janusgraph.pkgtest;

import org.junit.BeforeClass;
import org.junit.Test;

import org.janusgraph.CassandraStorageSetup;

public class CassandraThriftAssemblyIT extends AbstractJanusGraphAssemblyIT {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Test
    public void testCassandraThriftSimpleSession() throws Exception {
        testSimpleGremlinSession("conf/janusgraph-cassandra.properties", "cassandrathrift");
    }
}
