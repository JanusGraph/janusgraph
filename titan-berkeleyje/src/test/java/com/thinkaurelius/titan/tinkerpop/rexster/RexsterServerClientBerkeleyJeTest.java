package com.thinkaurelius.titan.tinkerpop.rexster;

import com.thinkaurelius.titan.BerkeleyJeStorageSetup;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class RexsterServerClientBerkeleyJeTest extends RexsterServerClientTest {

    public RexsterServerClientBerkeleyJeTest() {
        super(BerkeleyJeStorageSetup.getBerkeleyJEGraphConfiguration());
    }

}
