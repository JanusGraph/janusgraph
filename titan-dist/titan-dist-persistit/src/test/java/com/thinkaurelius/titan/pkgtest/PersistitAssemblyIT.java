package com.thinkaurelius.titan.pkgtest;

import org.junit.Test;

public class PersistitAssemblyIT extends AssemblyITSupport {
    
    @Test
    public void testPersistitSimpleSession() throws Exception {
        testSimpleGremlinSession("conf/titan-persistit.properties", "persistit");
    }
}