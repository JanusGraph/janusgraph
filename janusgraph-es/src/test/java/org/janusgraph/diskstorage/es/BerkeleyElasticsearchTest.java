package org.janusgraph.diskstorage.es;

import com.google.common.base.Joiner;
import org.janusgraph.StorageSetup;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.janusgraph.graphdb.JanusGraphIndexTest;
import org.janusgraph.util.system.IOUtils;
import org.junit.Test;

import java.io.File;

import static org.janusgraph.diskstorage.es.ElasticSearchIndex.*;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;
import static org.janusgraph.BerkeleyStorageSetup.getBerkeleyJEConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BerkeleyElasticsearchTest extends JanusGraphIndexTest {

    public BerkeleyElasticsearchTest() {
        super(true, true, true);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = getBerkeleyJEConfiguration();
        //Add index
        config.set(INDEX_BACKEND,"elasticsearch",INDEX);
        config.set(LOCAL_MODE,true,INDEX);
        config.set(CLIENT_ONLY,false,INDEX);
        config.set(INDEX_DIRECTORY, StorageSetup.getHomeDir("es"), INDEX);
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

    /**
     * Test {@link org.janusgraph.example.GraphOfTheGodsFactory#create(String)}.
     */
    @Test
    public void testGraphOfTheGodsFactoryCreate() {
        File bdbtmp = new File("target/gotgfactory");
        IOUtils.deleteDirectory(bdbtmp, true);

        JanusGraph gotg = GraphOfTheGodsFactory.create(bdbtmp.getPath());
        JanusGraphIndexTest.assertGraphOfTheGods(gotg);
        gotg.close();
    }
}
