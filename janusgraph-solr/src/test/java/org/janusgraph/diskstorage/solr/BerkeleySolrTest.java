package org.janusgraph.diskstorage.solr;

import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusIndexTest;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.janusgraph.BerkeleyStorageSetup.getBerkeleyJEConfiguration;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BerkeleySolrTest extends SolrJanusIndexTest {

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = getBerkeleyJEConfiguration();
        //Add index
        config.set(INDEX_BACKEND,"solr",INDEX);
        config.set(SolrIndex.ZOOKEEPER_URL, SolrRunner.getMiniCluster().getZkServer().getZkAddress(), INDEX);
        config.set(SolrIndex.WAIT_SEARCHER, true, INDEX);
        //TODO: set SOLR specific config options
        return config.getConfiguration();
    }

    @Override
    public boolean supportsWildcardQuery() {
        return false;
    }
}
