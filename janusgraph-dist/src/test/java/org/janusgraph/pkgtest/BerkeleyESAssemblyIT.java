package org.janusgraph.pkgtest;

import org.junit.Test;

public class BerkeleyESAssemblyIT extends AbstractTitanAssemblyIT {
    
    @Test
    public void testBerkeleyGettingStarted() throws Exception {
        testGettingStartedGremlinSession("conf/titan-berkeleyje-es.properties", "berkeleyje");
    }
}
