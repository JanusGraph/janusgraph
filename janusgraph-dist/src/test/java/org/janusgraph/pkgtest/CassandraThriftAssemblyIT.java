package org.janusgraph.pkgtest;

import org.junit.BeforeClass;
import org.junit.Test;

import org.janusgraph.CassandraStorageSetup;

public class CassandraThriftAssemblyIT extends AbstractJanusAssemblyIT {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Test
    public void testCassandraThriftSimpleSession() throws Exception {
        testSimpleGremlinSession("conf/janus-cassandra.properties", "cassandrathrift");
    }
}
