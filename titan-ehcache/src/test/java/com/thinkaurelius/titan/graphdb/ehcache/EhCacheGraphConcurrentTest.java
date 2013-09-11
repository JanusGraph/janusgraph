package com.thinkaurelius.titan.graphdb.ehcache;

import com.thinkaurelius.titan.EhCacheStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;

public class EhCacheGraphConcurrentTest extends TitanGraphConcurrentTest {
    public EhCacheGraphConcurrentTest() {
        super(EhCacheStorageSetup.getEhCacheGraphConfig());
    }
}
