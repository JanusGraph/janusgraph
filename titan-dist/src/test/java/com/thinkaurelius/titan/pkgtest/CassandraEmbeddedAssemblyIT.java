package com.thinkaurelius.titan.pkgtest;

import org.junit.Test;

public class CassandraEmbeddedAssemblyIT extends AbstractTitanAssemblyIT {

    @Test
    public void testEmbeddedCassandraSimpleSession() throws Exception {
        testSimpleGremlinSession("conf/titan-cassandra-embedded.properties", "embeddedcassandra");
    }
}
