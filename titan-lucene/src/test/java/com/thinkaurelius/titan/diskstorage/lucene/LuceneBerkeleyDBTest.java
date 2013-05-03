package com.thinkaurelius.titan.diskstorage.lucene;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.graphdb.TitanIndexTest;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class LuceneBerkeleyDBTest extends TitanIndexTest {

    public LuceneBerkeleyDBTest() {
        super(getLuceneBDBConfig(), true, true, true);
    }

    public static final Configuration getLuceneBDBConfig() {
        BaseConfiguration config = new BaseConfiguration();
        config.subset(STORAGE_NAMESPACE).addProperty(STORAGE_BACKEND_KEY, "com.thinkaurelius.titan.diskstorage.berkeleyje.BerkeleyJEStoreManager");
        config.subset(STORAGE_NAMESPACE).addProperty(STORAGE_DIRECTORY_KEY, StorageSetup.getHomeDir());
        //Add index
        Configuration sub = config.subset(STORAGE_NAMESPACE).subset(INDEX_NAMESPACE).subset(INDEX);
        sub.setProperty(INDEX_BACKEND_KEY,"com.thinkaurelius.titan.diskstorage.lucene.LuceneIndex");
        sub.setProperty(STORAGE_DIRECTORY_KEY, StorageSetup.getHomeDir("lucene"));
//        System.out.println(GraphDatabaseConfiguration.toString(config));
        return config;
    }



}
