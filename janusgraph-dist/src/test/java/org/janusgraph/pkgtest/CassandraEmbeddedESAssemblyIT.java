package org.janusgraph.pkgtest;

import org.junit.Test;

public class CassandraEmbeddedESAssemblyIT extends AbstractJanusAssemblyIT {

    @Test
    public void testEmbeddedCassandraGettingStarted() throws Exception {
        testGettingStartedGremlinSession("conf/janus-cassandra-embedded-es.properties", "embeddedcassandra");
    }
}
