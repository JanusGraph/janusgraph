package com.thinkaurelius.titan.pkgtest;

import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import org.junit.Test;
import java.io.File;

public class CassandraEmbeddedESAssemblyIT extends AssemblyITSupport {
    
    @Test
    public void testEmbeddedCassandraGettingStarted() throws Exception {
        testGettingStartedGremlinSession("conf/titan-cassandra-embedded-es.properties", "embeddedcassandra");
    }
}
