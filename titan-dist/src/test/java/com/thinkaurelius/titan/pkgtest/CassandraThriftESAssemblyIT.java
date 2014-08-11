package com.thinkaurelius.titan.pkgtest;

import com.thinkaurelius.titan.diskstorage.es.ElasticsearchRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.thinkaurelius.titan.CassandraStorageSetup;

public class CassandraThriftESAssemblyIT extends AbstractTitanAssemblyIT {

    public static final String ES_HOME = "../../titan-es";

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @BeforeClass
    public static void startES() {
        new ElasticsearchRunner(ES_HOME).start();
    }

    @Test
    public void testCassandraGettingStarted() throws Exception {
        testGettingStartedGremlinSession("conf/titan-cassandra-es.properties", "cassandrathrift");
    }

    @AfterClass
    public static void stopES() {
        new ElasticsearchRunner(ES_HOME).stop();
    }
}
