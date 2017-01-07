package org.janusgraph.diskstorage.solr;

import com.google.common.base.Joiner;
import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphIndexTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;

public class ThriftSolrTest extends SolrJanusGraphIndexTest {

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config =
                CassandraStorageSetup.getCassandraThriftConfiguration(ThriftSolrTest.class.getName());
        //Add index
        config.set(SolrIndex.ZOOKEEPER_URL, SolrRunner.getMiniCluster().getZkServer().getZkAddress(), INDEX);
        config.set(SolrIndex.WAIT_SEARCHER, true, INDEX);
        config.set(INDEX_BACKEND,"solr",INDEX);
        //TODO: set SOLR specific config options
        return config.getConfiguration();
    }

    @BeforeClass
    public static void beforeClass() {
        String userDir = System.getProperty("user.dir");
        String cassandraDirFormat = Joiner.on(File.separator).join(userDir, userDir.contains("janusgraph-solr")
                                        ? "target" : "janusgraph-solr/target", "cassandra", "%s", "localhost-murmur");

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

    @Override
    public boolean supportsWildcardQuery() {
        return false;
    }

}
