package com.thinkaurelius.titan.diskstorage.solr;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanIndexTest;

import static com.thinkaurelius.titan.diskstorage.solr.SolrIndex.*;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BerkeleySolrTest extends TitanIndexTest {

    public BerkeleySolrTest() {
        super(true, true, true);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = getBerkeleyJEConfiguration();
        //Add index
        config.set(INDEX_BACKEND,"solr",INDEX);
        //TODO: set SOLR specific config options
        config.set(INDEX_DIRECTORY,StorageSetup.getHomeDir("es"),INDEX);
        return config.getConfiguration();
    }

    public static ModifiableConfiguration getBerkeleyJEConfiguration() {
        return buildConfiguration()
                .set(STORAGE_BACKEND,"berkeleyje")
                .set(STORAGE_DIRECTORY, StorageSetup.getHomeDir());
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }
}