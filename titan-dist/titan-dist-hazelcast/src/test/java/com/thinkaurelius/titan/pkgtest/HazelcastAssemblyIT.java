package com.thinkaurelius.titan.pkgtest;

import org.junit.Test;

public class HazelcastAssemblyIT extends AssemblyITSupport {
    
    @Test
    public void testHazelcastSimpleSession() throws Exception {
        testSimpleGremlinSession("conf/titan-hazelcast.properties", "hazelcastcache");
    }
}