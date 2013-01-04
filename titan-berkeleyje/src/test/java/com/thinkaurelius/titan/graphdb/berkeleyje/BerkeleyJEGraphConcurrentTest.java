package com.thinkaurelius.titan.graphdb.berkeleyje;

import com.thinkaurelius.titan.BerkeleyJeStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;

public class BerkeleyJEGraphConcurrentTest extends TitanGraphConcurrentTest {

    public BerkeleyJEGraphConcurrentTest() {
        super(BerkeleyJeStorageSetup.getBerkeleyJEGraphConfiguration());
    }

}
