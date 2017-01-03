package org.janusgraph.pkgtest;

import org.junit.Test;

public class BerkeleyESAssemblyIT extends AbstractJanusAssemblyIT {
    
    @Test
    public void testBerkeleyGettingStarted() throws Exception {
        testGettingStartedGremlinSession("conf/janus-berkeleyje-es.properties", "berkeleyje");
    }
}
