package com.thinkaurelius.titan.graphdb.berkeleyje;

import com.thinkaurelius.titan.BerkeleyJeStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;

public class BerkeleyJEGraphPerformanceTest extends TitanGraphPerformanceTest {

    public BerkeleyJEGraphPerformanceTest() {
        super(BerkeleyJeStorageSetup.getBerkeleyJEGraphConfiguration(), 0, 1, false);
    }

}
