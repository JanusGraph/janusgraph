package com.thinkaurelius.titan.graphdb.persistit;

import com.thinkaurelius.titan.PersistitStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceMemoryTest;

public class PersistitGraphPerformanceMemoryTest extends TitanGraphPerformanceMemoryTest {

    public PersistitGraphPerformanceMemoryTest() {
        super(PersistitStorageSetup.getPersistitGraphConfig());
    }
}
