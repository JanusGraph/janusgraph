package com.thinkaurelius.titan.graphdb.astyanax;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.embedded.CassandraDaemonWrapper;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import org.junit.BeforeClass;

public class InternalAstyanaxGraphTest extends TitanGraphTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraDaemonWrapper.start(StorageSetup.cassandraYamlPath);
    }

    public InternalAstyanaxGraphTest() {
        super(StorageSetup.getAstyanaxGraphConfiguration());
    }

}
