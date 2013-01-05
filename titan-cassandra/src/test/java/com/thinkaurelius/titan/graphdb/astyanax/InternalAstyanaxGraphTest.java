package com.thinkaurelius.titan.graphdb.astyanax;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.embedded.CassandraDaemonWrapper;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import org.junit.BeforeClass;

public class InternalAstyanaxGraphTest extends TitanGraphTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraDaemonWrapper.start(CassandraStorageSetup.cassandraYamlPath);
    }

    public InternalAstyanaxGraphTest() {
        super(CassandraStorageSetup.getAstyanaxGraphConfiguration());
    }

}
