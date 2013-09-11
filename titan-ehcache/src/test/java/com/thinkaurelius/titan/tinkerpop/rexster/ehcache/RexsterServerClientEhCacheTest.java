package com.thinkaurelius.titan.tinkerpop.rexster.ehcache;

import com.thinkaurelius.titan.EhCacheStorageSetup;
import com.thinkaurelius.titan.tinkerpop.rexster.RexsterServerClientTest;

public class RexsterServerClientEhCacheTest extends RexsterServerClientTest {
    public RexsterServerClientEhCacheTest() {
        super(EhCacheStorageSetup.getEhCacheGraphConfig());
    }
}
