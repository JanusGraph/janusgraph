package com.thinkaurelius.titan.graphdb.persistit;

import com.thinkaurelius.titan.PersistitStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class PersistitGraphTest extends TitanGraphTest {
    public PersistitGraphTest() {
        super(PersistitStorageSetup.getPersistitGraphConfig());
    }
}
