package com.thinkaurelius.titan.graphdb.ehcache;

import com.thinkaurelius.titan.EhCacheStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;

public class EhCacheGraphPerformanceTest extends TitanGraphPerformanceTest {
    public EhCacheGraphPerformanceTest() {
        // Changed to match corresponding Berkeley JE test
        super(EhCacheStorageSetup.getEhCacheGraphConfig(), 2, 8, false);
    }
}
