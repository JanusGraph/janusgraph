package com.thinkaurelius.titan.graphdb.astyanax;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;
import org.junit.BeforeClass;

public class InternalAstyanaxGraphConcurrentTest extends TitanGraphConcurrentTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.cassandraYamlPath);
    }

    public InternalAstyanaxGraphConcurrentTest() {
        super(CassandraStorageSetup.getAstyanaxGraphConfiguration());
    }

}
