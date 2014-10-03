package com.thinkaurelius.titan.diskstorage.solr;

import com.google.common.base.Joiner;
import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanIndexTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;

public class ThriftSolrTest extends SolrTitanIndexTest {

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config =
                CassandraStorageSetup.getCassandraThriftConfiguration(ThriftSolrTest.class.getName());
        //Add index
        config.set(SolrIndex.ZOOKEEPER_URL, SolrRunner.getMiniCluster().getZkServer().getZkAddress(), INDEX);

        config.set(INDEX_BACKEND,"solr",INDEX);
        //TODO: set SOLR specific config options
        return config.getConfiguration();
    }

    @BeforeClass
    public static void beforeClass() {
        String userDir = System.getProperty("user.dir");
        String cassandraDirFormat = Joiner.on(File.separator).join(userDir, userDir.contains("titan-solr")
                                        ? "target" : "titan-solr/target", "cassandra", "%s", "localhost-rp");

        System.setProperty("test.cassandra.confdir", String.format(cassandraDirFormat, "conf"));
        System.setProperty("test.cassandra.datadir", String.format(cassandraDirFormat, "data"));

        CassandraStorageSetup.startCleanEmbedded();
    }


    /*
    The following two test cases do not pass for Solr since there is no (performant) way of checking
    whether the document has been deleted before doing an update which will re-create the document.
     */

    @Override
    public void testDeleteVertexThenAddProperty() throws BackendException {

    }


    @Override
    public void testDeleteVertexThenModifyProperty() throws BackendException {

    }


}
