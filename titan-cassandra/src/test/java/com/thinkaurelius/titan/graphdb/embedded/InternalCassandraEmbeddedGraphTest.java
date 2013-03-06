package com.thinkaurelius.titan.graphdb.embedded;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class InternalCassandraEmbeddedGraphTest extends TitanGraphTest {

    public InternalCassandraEmbeddedGraphTest() {
        super(CassandraStorageSetup.getEmbeddedCassandraPartitionGraphConfiguration());
    }

    @Override
    public void testIteration() {
        // do nothing as this is uses ordered Partitioner
    }

}
