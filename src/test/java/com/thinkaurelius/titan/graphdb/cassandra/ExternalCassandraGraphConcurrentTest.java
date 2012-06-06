package com.thinkaurelius.titan.graphdb.cassandra;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.diskstorage.cassandra.CassandraLocalhostHelper;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;

public class ExternalCassandraGraphConcurrentTest extends TitanGraphConcurrentTest {

    public static CassandraLocalhostHelper ch = new CassandraLocalhostHelper();

    public ExternalCassandraGraphConcurrentTest() {
        super(ch.getConfiguration());
    }

    @BeforeClass
    public static void beforeClass() {
        ch.startCassandra();
    }

    @AfterClass
    public static void afterClass() throws InterruptedException {
        ch.stopCassandra();
    }

    public void cleanUp() {
        CassandraThriftStorageManager.dropKeyspace(
                CassandraThriftStorageManager.KEYSPACE_DEFAULT,
                "127.0.0.1",
                CassandraThriftStorageManager.PORT_DEFAULT);
    }

}