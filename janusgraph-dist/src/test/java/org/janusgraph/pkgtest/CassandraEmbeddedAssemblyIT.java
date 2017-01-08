package org.janusgraph.pkgtest;

import org.junit.Test;

public class CassandraEmbeddedAssemblyIT extends AbstractJanusGraphAssemblyIT {

    @Test
    public void testEmbeddedCassandraSimpleSession() throws Exception {
        testSimpleGremlinSession("conf/janusgraph-cassandra-embedded.properties", "embeddedcassandra");
    }
}
