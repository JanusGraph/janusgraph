package com.thinkaurelius.titan.pkgtest;

import org.junit.Test;
import java.io.File;

public class CassandraEmbeddedAssemblyIT extends AssemblyITSupport {
    
    @Test
    public void testEmbeddedCassandraSimpleSession() throws Exception {
        testSimpleGremlinSession("conf/titan-cassandra-embedded.properties", "embeddedcassandra");
    }
}
