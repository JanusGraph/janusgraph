package com.thinkaurelius.titan.graphdb.persistit;

import com.thinkaurelius.titan.PersistitStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;

public class PersistitGraphConcurrentTest extends TitanGraphConcurrentTest {
    public PersistitGraphConcurrentTest() {
        super(PersistitStorageSetup.getPersistitGraphConfig());
    }
}
