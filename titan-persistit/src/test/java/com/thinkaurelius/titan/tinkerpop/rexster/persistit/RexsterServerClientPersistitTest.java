package com.thinkaurelius.titan.tinkerpop.rexster.persistit;

import com.thinkaurelius.titan.PersistitStorageSetup;
import com.thinkaurelius.titan.tinkerpop.rexster.RexsterServerClientTest;

public class RexsterServerClientPersistitTest extends RexsterServerClientTest {
    public RexsterServerClientPersistitTest() {
        super(PersistitStorageSetup.getPersistitGraphConfig());
    }
}
