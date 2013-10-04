package com.thinkaurelius.titan.graphdb.thrift;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;
import com.thinkaurelius.titan.testcategory.PerformanceTests;

import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@Category({PerformanceTests.class})
public class InternalCassandraGraphConcurrentTest extends TitanGraphConcurrentTest {

    public InternalCassandraGraphConcurrentTest() {
        super(CassandraStorageSetup.getCassandraThriftGraphConfiguration(InternalCassandraGraphConcurrentTest.class.getSimpleName()));
    }

    @BeforeClass
    public static void beforeClass() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }
}
