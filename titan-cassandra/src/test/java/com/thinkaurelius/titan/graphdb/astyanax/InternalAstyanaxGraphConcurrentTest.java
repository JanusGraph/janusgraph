package com.thinkaurelius.titan.graphdb.astyanax;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;
import com.thinkaurelius.titan.testcategory.PerformanceTests;

import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@Category({PerformanceTests.class})
public class InternalAstyanaxGraphConcurrentTest extends TitanGraphConcurrentTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }

    public InternalAstyanaxGraphConcurrentTest() {
        super(CassandraStorageSetup.getAstyanaxGraphConfiguration(InternalAstyanaxGraphConcurrentTest.class.getSimpleName()));
    }

}
