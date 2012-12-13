package com.thinkaurelius.titan.graphdb.cassandra;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class InternalCassandraEmbeddedGraphPerformanceTest extends TitanGraphPerformanceTest {

    public InternalCassandraEmbeddedGraphPerformanceTest() {
        super(StorageSetup.getEmbeddedCassandraPartitionGraphConfiguration(),0,1,false);
    }

}
