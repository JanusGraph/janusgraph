package com.thinkaurelius.titan.pkgtest;

import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.File;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.CassandraThriftStoreManager;

public class CassandraThriftESAssemblyIT extends AssemblyITSupport {

    @BeforeClass
    public static void startCassandra() {
        String cassyaml = BUILD_DIR + File.separator + "cassandra" + File.separator + "conf" +
                File.separator + "localhost-rp" + File.separator + "cassandra.yaml";
        CassandraProcessStarter.startCleanEmbedded("file://" + cassyaml);
    }
    
    @After
    public void clearData() throws Exception {
        ModifiableConfiguration c = GraphDatabaseConfiguration.buildConfiguration();
        CassandraThriftStoreManager m = new CassandraThriftStoreManager(c);
        m.clearStorage();
    }

    @Test
    public void testCassandraGettingStarted() throws Exception {
        testGettingStartedGremlinSession("conf/titan-cassandra-es.properties", "cassandrathrift");
    }
}
