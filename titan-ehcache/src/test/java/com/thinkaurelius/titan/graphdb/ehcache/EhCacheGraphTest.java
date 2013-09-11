package com.thinkaurelius.titan.graphdb.ehcache;

import com.thinkaurelius.titan.EhCacheStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class EhCacheGraphTest extends TitanGraphTest {
    public EhCacheGraphTest() {
        super(EhCacheStorageSetup.getEhCacheGraphConfig());
    }
}
