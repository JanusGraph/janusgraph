package com.thinkaurelius.titan.pkgtest;

import org.junit.BeforeClass;
import org.junit.Test;

import com.thinkaurelius.titan.CassandraStorageSetup;

public class CassandraThriftAssemblyIT extends AbstractTitanAssemblyIT {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Test
    public void testCassandraThriftSimpleSession() throws Exception {
        testSimpleGremlinSession("conf/titan-cassandra.properties", "cassandrathrift");
    }
}
