package com.thinkaurelius.titan.graphdb.cassandra;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class InternalCassandraEmbeddedGraphConcurrentTest extends TitanGraphConcurrentTest {

    public InternalCassandraEmbeddedGraphConcurrentTest() {
        super(StorageSetup.getEmbeddedCassandraGraphConfiguration());
    }

}
