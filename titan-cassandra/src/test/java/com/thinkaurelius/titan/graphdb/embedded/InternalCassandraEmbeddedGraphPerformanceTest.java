package com.thinkaurelius.titan.graphdb.embedded;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InternalCassandraEmbeddedGraphPerformanceTest extends TitanGraphPerformanceTest {

    public InternalCassandraEmbeddedGraphPerformanceTest() {
        super(CassandraStorageSetup.getEmbeddedCassandraPartitionGraphConfiguration(InternalCassandraEmbeddedGraphPerformanceTest.class.getSimpleName()),0,1,false);
    }

}
