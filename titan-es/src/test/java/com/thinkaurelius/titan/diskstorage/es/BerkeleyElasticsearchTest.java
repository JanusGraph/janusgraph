package com.thinkaurelius.titan.diskstorage.es;

import com.google.common.base.Joiner;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.example.GraphOfTheGodsFactory;
import com.thinkaurelius.titan.graphdb.TitanIndexTest;
import org.junit.Test;

import java.io.File;

import static com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex.*;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;
import static com.thinkaurelius.titan.BerkeleyStorageSetup.getBerkeleyJEConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BerkeleyElasticsearchTest extends TitanIndexTest {

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
        config.set(INDEX_DIRECTORY,StorageSetup.getHomeDir("es"),INDEX);
        return config.getConfiguration();
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }

    /**
     * Test {@link com.thinkaurelius.titan.example.GraphOfTheGodsFactory#create(String)}.
     */
    @Test
    public void testGraphOfTheGodsFactoryCreate() {
        String bdbtmp = Joiner.on(File.separator).join("target", "gotgfactory");
        TitanGraph gotg = GraphOfTheGodsFactory.create(bdbtmp);
        TitanIndexTest.assertGraphOfTheGods(gotg);
        gotg.shutdown();
    }
}