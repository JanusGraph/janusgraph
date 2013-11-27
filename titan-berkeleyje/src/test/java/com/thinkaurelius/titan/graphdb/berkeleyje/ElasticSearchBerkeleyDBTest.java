package com.thinkaurelius.titan.graphdb.berkeleyje;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.graphdb.TitanIndexTest;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import static com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex.CLIENT_ONLY_KEY;
import static com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex.LOCAL_MODE_KEY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ElasticSearchBerkeleyDBTest extends TitanIndexTest {

    public ElasticSearchBerkeleyDBTest() {
        super(getElasticSearchBDBConfig(), true, true, true);
    }

    public static final Configuration getElasticSearchBDBConfig() {
        BaseConfiguration config = new BaseConfiguration();
        config.subset(STORAGE_NAMESPACE).addProperty(STORAGE_BACKEND_KEY, "com.thinkaurelius.titan.diskstorage.berkeleyje.BerkeleyJEStoreManager");
        config.subset(STORAGE_NAMESPACE).addProperty(STORAGE_DIRECTORY_KEY, StorageSetup.getHomeDir());
        //Add index
        Configuration sub = config.subset(STORAGE_NAMESPACE).subset(INDEX_NAMESPACE).subset(INDEX);
        sub.setProperty(INDEX_BACKEND_KEY,"com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex");
        sub.setProperty(LOCAL_MODE_KEY,true);
        sub.setProperty(CLIENT_ONLY_KEY,false);
        sub.setProperty(STORAGE_DIRECTORY_KEY, StorageSetup.getHomeDir("es"));
//        System.out.println(GraphDatabaseConfiguration.toString(config));
        return config;
    }


    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }
}