package com.thinkaurelius.titan.graphdb.embedded;

import org.junit.experimental.categories.Category;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;
import com.thinkaurelius.titan.testcategory.PerformanceTests;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

@Category({PerformanceTests.class})
public class InternalCassandraEmbeddedGraphPerformanceTest extends TitanGraphPerformanceTest {

    public InternalCassandraEmbeddedGraphPerformanceTest() {
        super(CassandraStorageSetup.getEmbeddedCassandraPartitionGraphConfiguration(InternalCassandraEmbeddedGraphPerformanceTest.class.getSimpleName()),0,1,false);
    }

}
