package com.thinkaurelius.titan.graphdb.embedded;

import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import org.junit.experimental.categories.Category;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceMemoryTest;
import com.thinkaurelius.titan.testcategory.PerformanceTests;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InternalCassandraEmbeddedGraphMemoryPerformanceTest extends TitanGraphPerformanceMemoryTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return CassandraStorageSetup.getEmbeddedCassandraPartitionGraphConfiguration(getClass().getSimpleName());
    }

}
