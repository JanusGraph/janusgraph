package com.thinkaurelius.titan.pkgtest;

import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.File;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.CassandraThriftStoreManager;

public class CassandraThriftAssemblyIT extends AssemblyITSupport {

    @BeforeClass
    public static void startCassandra() {
        String cassyaml = BUILD_DIR + File.separator + "cassandra" + File.separator + "conf" +
                File.separator + "localhost-rp" + File.separator + "cassandra.yaml";
        CassandraProcessStarter.startCleanEmbedded("file://" + cassyaml);
    }
    
    @After
    public void clearData() throws Exception {
        Configuration c = new BaseConfiguration();
        c.setProperty("hostname", "127.0.0.1");
        CassandraThriftStoreManager m = new CassandraThriftStoreManager(c);
        m.clearStorage();
    }

    @Test
    public void testCassandraThriftSimpleSession() throws Exception {
        testSimpleGremlinSession("conf/titan-cassandra.properties", "cassandrathrift");
    }
    
    @Test
    public void testCassandraThriftGettingStarted() throws Exception {
        testGettingStartedGremlinSession("conf/titan-cassandra-es.properties", "cassandrathrift");
    }
}