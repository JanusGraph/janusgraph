package com.thinkaurelius.titan.graphdb.cassandra;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.embedded.CassandraDaemonWrapper;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import org.junit.BeforeClass;

public class InternalCassandraGraphTest extends TitanGraphTest {

    public InternalCassandraGraphTest() {
        super(StorageSetup.getCassandraThriftGraphConfiguration());
    }

    @BeforeClass
    public static void beforeClass() {
        CassandraDaemonWrapper.start(StorageSetup.cassandraYamlPath);
    }
}
