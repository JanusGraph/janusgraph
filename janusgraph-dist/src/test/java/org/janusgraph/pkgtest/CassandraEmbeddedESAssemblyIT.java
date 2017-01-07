package org.janusgraph.pkgtest;

import org.junit.Test;

public class CassandraEmbeddedESAssemblyIT extends AbstractJanusGraphAssemblyIT {

    @Test
    public void testEmbeddedCassandraGettingStarted() throws Exception {
        testGettingStartedGremlinSession("conf/janusgraph-cassandra-embedded-es.properties", "embeddedcassandra");
    }
}
