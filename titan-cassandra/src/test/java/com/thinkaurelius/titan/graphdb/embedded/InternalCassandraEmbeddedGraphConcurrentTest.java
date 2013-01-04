package com.thinkaurelius.titan.graphdb.embedded;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class InternalCassandraEmbeddedGraphConcurrentTest extends TitanGraphConcurrentTest {

    public InternalCassandraEmbeddedGraphConcurrentTest() {
        super(CassandraStorageSetup.getEmbeddedCassandraPartitionGraphConfiguration());
    }

}
