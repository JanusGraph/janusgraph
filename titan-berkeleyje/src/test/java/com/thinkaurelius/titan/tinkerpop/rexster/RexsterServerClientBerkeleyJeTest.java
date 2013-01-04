package com.thinkaurelius.titan.tinkerpop.rexster;

import com.thinkaurelius.titan.StorageSetup;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class RexsterServerClientBerkeleyJeTest extends RexsterServerClientTest {

    public RexsterServerClientBerkeleyJeTest() {
        super(StorageSetup.getBerkeleyJEGraphConfiguration());
    }

}
