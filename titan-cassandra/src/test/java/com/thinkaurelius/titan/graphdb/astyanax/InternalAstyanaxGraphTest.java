package com.thinkaurelius.titan.graphdb.astyanax;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import com.thinkaurelius.titan.testcategory.RandomPartitionerTests;

import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@Category({RandomPartitionerTests.class})
public class InternalAstyanaxGraphTest extends TitanGraphTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.cassandraYamlPath);
    }

    public InternalAstyanaxGraphTest() {
        super(CassandraStorageSetup.getAstyanaxGraphConfiguration(InternalAstyanaxGraphTest.class.getSimpleName()));
    }

}
