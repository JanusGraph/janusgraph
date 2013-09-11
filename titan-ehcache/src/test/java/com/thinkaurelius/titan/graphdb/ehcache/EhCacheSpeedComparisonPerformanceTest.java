package com.thinkaurelius.titan.graphdb.ehcache;

import com.thinkaurelius.titan.EhCacheStorageSetup;
import com.thinkaurelius.titan.graphdb.SpeedComparisonPerformanceTest;

public class EhCacheSpeedComparisonPerformanceTest extends SpeedComparisonPerformanceTest {
    public EhCacheSpeedComparisonPerformanceTest() {
        super(EhCacheStorageSetup.getEhCacheGraphConfig());
    }
}
