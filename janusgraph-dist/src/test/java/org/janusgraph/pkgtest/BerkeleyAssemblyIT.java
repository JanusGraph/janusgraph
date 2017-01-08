package org.janusgraph.pkgtest;

import org.junit.Test;

public class BerkeleyAssemblyIT extends AbstractJanusGraphAssemblyIT {
    
    @Test
    public void testBerkeleySimpleSession() throws Exception {
        testSimpleGremlinSession("conf/janusgraph-berkeleyje.properties", "berkeleyje");
    }
}
