package com.thinkaurelius.titan.pkgtest;

import org.junit.Test;

public class CassandraEmbeddedESAssemblyIT extends AbstractTitanAssemblyIT {

    @Test
    public void testEmbeddedCassandraGettingStarted() throws Exception {
        testGettingStartedGremlinSession("conf/titan-cassandra-embedded-es.properties", "embeddedcassandra");
    }
}
