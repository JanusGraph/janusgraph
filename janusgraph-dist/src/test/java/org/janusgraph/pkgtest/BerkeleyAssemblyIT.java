package org.janusgraph.pkgtest;

import org.junit.Test;

public class BerkeleyAssemblyIT extends AbstractJanusAssemblyIT {
    
    @Test
    public void testBerkeleySimpleSession() throws Exception {
        testSimpleGremlinSession("conf/janus-berkeleyje.properties", "berkeleyje");
    }
}
