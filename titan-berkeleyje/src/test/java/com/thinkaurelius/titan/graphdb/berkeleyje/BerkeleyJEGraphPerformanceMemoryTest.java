package com.thinkaurelius.titan.graphdb.berkeleyje;

import com.thinkaurelius.titan.BerkeleyJeStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceMemoryTest;

public class BerkeleyJEGraphPerformanceMemoryTest extends TitanGraphPerformanceMemoryTest {

    public BerkeleyJEGraphPerformanceMemoryTest() {
        super(BerkeleyJeStorageSetup.getBerkeleyJEPerformanceConfiguration());
    }

}
