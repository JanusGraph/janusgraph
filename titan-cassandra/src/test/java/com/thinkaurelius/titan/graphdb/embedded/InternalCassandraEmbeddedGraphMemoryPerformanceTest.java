package com.thinkaurelius.titan.graphdb.embedded;

import org.junit.experimental.categories.Category;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceMemoryTest;
import com.thinkaurelius.titan.testcategory.PerformanceTests;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InternalCassandraEmbeddedGraphMemoryPerformanceTest extends TitanGraphPerformanceMemoryTest {

    public InternalCassandraEmbeddedGraphMemoryPerformanceTest() {
        super(CassandraStorageSetup.getEmbeddedCassandraPartitionGraphConfiguration(InternalCassandraEmbeddedGraphMemoryPerformanceTest.class.getSimpleName()));
    }

}
