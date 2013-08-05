package com.thinkaurelius.titan.graphdb.thrift;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import com.thinkaurelius.titan.testcategory.RandomPartitionerTests;

import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@Category({RandomPartitionerTests.class})
public class InternalCassandraGraphTest extends TitanGraphTest {

    public InternalCassandraGraphTest() {
        super(CassandraStorageSetup.getCassandraThriftGraphConfiguration(InternalCassandraGraphTest.class.getSimpleName()));
    }

    @BeforeClass
    public static void beforeClass() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.cassandraYamlPath);
    }
}
