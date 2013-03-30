package com.thinkaurelius.titan.graphdb.persistit;

import com.thinkaurelius.titan.PersistitStorageSetup;
import com.thinkaurelius.titan.graphdb.SpeedComparisonPerformanceTest;

public class PersistitSpeedComparisonPerformanceTest extends SpeedComparisonPerformanceTest {
    public PersistitSpeedComparisonPerformanceTest() {
        super(PersistitStorageSetup.getPersistitGraphConfig());
    }
}
