package com.thinkaurelius.titan.graphdb.astyanax;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

public class InternalAstyanaxGraphTest extends TitanGraphTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }

    public InternalAstyanaxGraphTest() {
        super(CassandraStorageSetup.getAstyanaxGraphConfiguration(InternalAstyanaxGraphTest.class.getSimpleName()));
    }

}
