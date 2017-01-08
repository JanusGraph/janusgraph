package org.janusgraph.pkgtest;

import org.junit.Test;

public class BerkeleyESAssemblyIT extends AbstractJanusGraphAssemblyIT {
    
    @Test
    public void testBerkeleyGettingStarted() throws Exception {
        testGettingStartedGremlinSession("conf/janusgraph-berkeleyje-es.properties", "berkeleyje");
    }
}
