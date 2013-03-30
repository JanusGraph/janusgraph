package com.thinkaurelius.titan.graphdb.persistit;

import com.thinkaurelius.titan.PersistitStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;

public class PersistitGraphPerformanceTest extends TitanGraphPerformanceTest {
    public PersistitGraphPerformanceTest() {
        super(PersistitStorageSetup.getPersistitGraphConfig());
    }
}
