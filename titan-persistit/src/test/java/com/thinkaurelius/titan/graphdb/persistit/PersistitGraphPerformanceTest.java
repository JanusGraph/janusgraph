package com.thinkaurelius.titan.graphdb.persistit;

import com.thinkaurelius.titan.PersistitStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;

public class PersistitGraphPerformanceTest extends TitanGraphPerformanceTest {
    public PersistitGraphPerformanceTest() {
        // Changed to match corresponding Berkeley JE test
        super(PersistitStorageSetup.getPersistitGraphConfig(), 2, 8, false);
    }
}
