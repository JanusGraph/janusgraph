package com.thinkaurelius.titan.pkgtest;

import org.junit.BeforeClass;
import org.junit.Test;

import com.thinkaurelius.titan.CassandraStorageSetup;

public class CassandraThriftESAssemblyIT extends AbstractTitanAssemblyIT {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Test
    public void testCassandraGettingStarted() throws Exception {
        testGettingStartedGremlinSession("conf/titan-cassandra-es.properties", "cassandrathrift");
    }
}
