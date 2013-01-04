package com.thinkaurelius.titan.graphdb.cassandra;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.embedded.CassandraDaemonWrapper;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;
import org.junit.BeforeClass;

public class InternalCassandraGraphConcurrentTest extends TitanGraphConcurrentTest {

    public InternalCassandraGraphConcurrentTest() {
        super(StorageSetup.getCassandraThriftGraphConfiguration());
    }

    @BeforeClass
    public static void beforeClass() {
        CassandraDaemonWrapper.start(StorageSetup.cassandraYamlPath);
    }
}
