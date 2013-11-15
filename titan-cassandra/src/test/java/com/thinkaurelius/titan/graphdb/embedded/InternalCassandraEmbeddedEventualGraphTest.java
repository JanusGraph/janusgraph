package com.thinkaurelius.titan.graphdb.embedded;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanEventualGraphTest;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class InternalCassandraEmbeddedEventualGraphTest extends TitanEventualGraphTest {

    public InternalCassandraEmbeddedEventualGraphTest() {
        super(CassandraStorageSetup.getEmbeddedCassandraPartitionGraphConfiguration(InternalCassandraEmbeddedEventualGraphTest.class.getSimpleName()));
    }

}
