package com.thinkaurelius.titan.pkgtest;

import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import org.junit.Test;
import java.io.File;

public class CassandraEmbeddedAssemblyIT extends AssemblyITSupport {
    
    @Test
    public void testEmbeddedCassandraESGettingStarted() throws Exception {
        testGettingStartedGremlinSession("conf/titan-cassandra-embedded-es.properties", "embeddedcassandra");
    }
    
    @Test
    public void testEmbeddedCassandraSimpleSession() throws Exception {
        testSimpleGremlinSession("conf/titan-cassandra-embedded.properties", "embeddedcassandra");
    }
}
