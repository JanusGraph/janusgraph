package com.thinkaurelius.titan.pkgtest;

import org.junit.Test;
import java.io.File;

public class CassandraEmbeddedESAssemblyIT extends AssemblyITSupport {
    
    @Test
    public void testEmbeddedCassandraGettingStarted() throws Exception {
        testGettingStartedGremlinSession("conf/titan-cassandra-embedded-es.properties", "embeddedcassandra");
    }
}
