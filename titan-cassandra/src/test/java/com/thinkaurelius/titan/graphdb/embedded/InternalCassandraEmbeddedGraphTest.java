package com.thinkaurelius.titan.graphdb.embedded;

import org.junit.experimental.categories.Category;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class InternalCassandraEmbeddedGraphTest extends TitanGraphTest {

    public InternalCassandraEmbeddedGraphTest() {
        super(CassandraStorageSetup.getEmbeddedCassandraPartitionGraphConfiguration(InternalCassandraEmbeddedGraphTest.class.getSimpleName()));
    }

}
