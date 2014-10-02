package com.thinkaurelius.titan.diskstorage.solr;

import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanIndexTest;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import static com.thinkaurelius.titan.BerkeleyStorageSetup.getBerkeleyJEConfiguration;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BerkeleySolrTest extends SolrTitanIndexTest {

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = getBerkeleyJEConfiguration();
        //Add index
        config.set(INDEX_BACKEND,"solr",INDEX);
        config.set(SolrIndex.ZOOKEEPER_URL, SolrRunner.getMiniCluster().getZkServer().getZkAddress(), INDEX);
        //TODO: set SOLR specific config options
        return config.getConfiguration();
    }
}