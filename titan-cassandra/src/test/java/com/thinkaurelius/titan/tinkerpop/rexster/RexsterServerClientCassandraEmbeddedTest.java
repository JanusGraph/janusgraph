package com.thinkaurelius.titan.tinkerpop.rexster;

import com.thinkaurelius.titan.StorageSetup;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class RexsterServerClientCassandraEmbeddedTest extends RexsterServerClientTest {
    public RexsterServerClientCassandraEmbeddedTest() {
        super(StorageSetup.getEmbeddedCassandraPartitionGraphConfiguration());
    }

}
