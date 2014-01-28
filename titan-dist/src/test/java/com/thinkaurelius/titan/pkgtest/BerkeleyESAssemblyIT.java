package com.thinkaurelius.titan.pkgtest;

import org.junit.Test;

public class BerkeleyESAssemblyIT extends AssemblyITSupport {
    
    @Test
    public void testBerkeleyGettingStarted() throws Exception {
        testGettingStartedGremlinSession("conf/titan-berkeleydb-es.properties", "berkeleyje");
    }
}
