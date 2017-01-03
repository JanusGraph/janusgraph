package org.janusgraph.diskstorage.es;


import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusIndexTest;
import org.junit.BeforeClass;

import static org.janusgraph.CassandraStorageSetup.*;
import static org.janusgraph.diskstorage.es.ElasticSearchIndex.CLIENT_ONLY;
import static org.janusgraph.diskstorage.es.ElasticSearchIndex.LOCAL_MODE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_DIRECTORY;

public class ThriftElasticsearchTest extends JanusIndexTest {

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

    @Override
    public boolean supportsWildcardQuery() {
        return true;
    }
    @Override
    protected boolean supportsCollections() {
        return true;
    }

    @BeforeClass
    public static void beforeClass() {
        CassandraStorageSetup.startCleanEmbedded();
    }
}
