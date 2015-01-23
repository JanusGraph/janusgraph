package com.thinkaurelius.titan.pkgtest;

import org.junit.Test;

public class BerkeleyAssemblyIT extends AbstractTitanAssemblyIT {
    
    @Test
    public void testBerkeleySimpleSession() throws Exception {
        testSimpleGremlinSession("conf/titan-berkeleyje.properties", "berkeleyje");
    }
}
