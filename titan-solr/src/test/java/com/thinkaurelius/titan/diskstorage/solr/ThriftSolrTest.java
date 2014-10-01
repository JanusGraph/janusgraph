package com.thinkaurelius.titan.diskstorage.solr;

import com.google.common.base.Joiner;
import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanIndexTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;

public class ThriftSolrTest extends TitanIndexTest {

    public ThriftSolrTest() {
        super(true, true, true);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config =
                CassandraStorageSetup.getCassandraThriftConfiguration(ThriftSolrTest.class.getName());
        //Add index
        config.set(SolrIndex.ZOOKEEPER_URL, SolrRunner.getMiniCluster().getZkServer().getZkAddress(), INDEX);
        config.set(SolrIndex.CORES, SolrRunner.CORES, INDEX);
        config.set(SolrIndex.KEY_FIELD_NAMES, SolrRunner.KEY_FIELDS, INDEX);

        config.set(INDEX_BACKEND,"solr",INDEX);
        //TODO: set SOLR specific config options
        return config.getConfiguration();
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }


    @BeforeClass
    public static void setUpMiniCluster() throws Exception {
        SolrRunner.start();
    }

    @AfterClass
    public static void tearDownMiniCluster() throws Exception {
        SolrRunner.stop();
    }

    @BeforeClass
    public static void beforeClass() {
        System.setProperty("test.cassandra.confdir", Joiner.on(File.separator).join(
                System.getProperty("user.dir"), "target", "cassandra", "conf", "localhost-rp"));
        System.setProperty("test.cassandra.datadir", Joiner.on(File.separator).join(
                System.getProperty("user.dir"), "target", "cassandra", "data", "localhost-rp"));
        CassandraStorageSetup.startCleanEmbedded();
    }
}
