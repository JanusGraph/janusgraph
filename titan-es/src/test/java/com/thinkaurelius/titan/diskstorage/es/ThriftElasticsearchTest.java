package com.thinkaurelius.titan.diskstorage.es;


import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanIndexTest;
import org.junit.BeforeClass;

import static com.thinkaurelius.titan.CassandraStorageSetup.*;
import static com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex.CLIENT_ONLY;
import static com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex.LOCAL_MODE;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.INDEX_DIRECTORY;

public class ThriftElasticsearchTest extends TitanIndexTest {

    public ThriftElasticsearchTest() {
        super(true, true, true);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config =
                getCassandraThriftConfiguration(ThriftElasticsearchTest.class.getName());
        //Add index
        config.set(INDEX_BACKEND,"elasticsearch",INDEX);
        config.set(LOCAL_MODE,true,INDEX);
        config.set(CLIENT_ONLY,false,INDEX);
        config.set(INDEX_DIRECTORY, StorageSetup.getHomeDir("es"),INDEX);
        return config.getConfiguration();
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }

    @BeforeClass
    public static void beforeClass() {
        CassandraStorageSetup.startCleanEmbedded();
    }
}
