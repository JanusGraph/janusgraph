package org.janusgraph.pkgtest;

import org.junit.Test;

public class CassandraEmbeddedAssemblyIT extends AbstractJanusAssemblyIT {

    @Test
    public void testEmbeddedCassandraSimpleSession() throws Exception {
        testSimpleGremlinSession("conf/janus-cassandra-embedded.properties", "embeddedcassandra");
    }
}
