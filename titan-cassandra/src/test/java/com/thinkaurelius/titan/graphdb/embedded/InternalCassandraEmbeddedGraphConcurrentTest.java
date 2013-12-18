package com.thinkaurelius.titan.graphdb.embedded;

import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import org.junit.experimental.categories.Category;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;
import com.thinkaurelius.titan.testcategory.PerformanceTests;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

@Category({PerformanceTests.class})
public class InternalCassandraEmbeddedGraphConcurrentTest extends TitanGraphConcurrentTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return CassandraStorageSetup.getEmbeddedCassandraPartitionGraphConfiguration(getClass().getSimpleName());
    }

}
