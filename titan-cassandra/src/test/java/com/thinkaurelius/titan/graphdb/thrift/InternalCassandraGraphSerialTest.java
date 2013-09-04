package com.thinkaurelius.titan.graphdb.thrift;

import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.TitanGraphSerialTest;
import com.thinkaurelius.titan.testcategory.RandomPartitionerTests;

@Category({RandomPartitionerTests.class})
public class InternalCassandraGraphSerialTest extends TitanGraphSerialTest {

    public InternalCassandraGraphSerialTest() throws StorageException {
        super(CassandraStorageSetup.getCassandraThriftGraphConfiguration(InternalCassandraGraphSerialTest.class.getSimpleName()));
    }
    
    @BeforeClass
    public static void beforeClass() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.cassandraYamlPath);
    }
}