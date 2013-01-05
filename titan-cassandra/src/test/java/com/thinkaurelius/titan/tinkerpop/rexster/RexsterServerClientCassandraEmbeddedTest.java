package com.thinkaurelius.titan.tinkerpop.rexster;

import com.thinkaurelius.titan.CassandraStorageSetup;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class RexsterServerClientCassandraEmbeddedTest extends RexsterServerClientTest {
    public RexsterServerClientCassandraEmbeddedTest() {
        super(CassandraStorageSetup.getEmbeddedCassandraPartitionGraphConfiguration());
    }

}
