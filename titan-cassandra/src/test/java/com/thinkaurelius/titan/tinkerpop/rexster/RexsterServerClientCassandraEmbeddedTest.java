package com.thinkaurelius.titan.tinkerpop.rexster;

import com.thinkaurelius.titan.CassandraStorageSetup;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class RexsterServerClientCassandraEmbeddedTest extends RexsterServerClientTest {
    public RexsterServerClientCassandraEmbeddedTest() {
        super(CassandraStorageSetup.getEmbeddedCassandraPartitionGraphConfiguration(RexsterServerClientCassandraEmbeddedTest.class.getSimpleName()));
    }

}
