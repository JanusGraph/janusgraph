package org.janusgraph.pkgtest;

import org.janusgraph.diskstorage.es.ElasticsearchRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.janusgraph.CassandraStorageSetup;

public class CassandraThriftESAssemblyIT extends AbstractJanusAssemblyIT {

    public static final String ES_HOME = "../../janus-es";

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
        testGettingStartedGremlinSession("conf/janus-cassandra-es.properties", "cassandrathrift");
    }

    @AfterClass
    public static void stopES() {
        new ElasticsearchRunner(ES_HOME).stop();
    }
}
