package com.thinkaurelius.titan.pkgtest;

import org.junit.Test;

public class BerkeleyAssemblyIT extends AssemblyITSupport {
    
    @Test
    public void testBerkeleySimpleSession() throws Exception {
        testSimpleGremlinSession("conf/titan-berkeleydb.properties", "berkeleyje");
    }
}