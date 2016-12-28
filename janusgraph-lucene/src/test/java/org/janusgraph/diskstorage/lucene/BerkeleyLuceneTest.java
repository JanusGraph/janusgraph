package org.janusgraph.diskstorage.lucene;

import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.TitanIndexTest;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_DIRECTORY;
import static org.janusgraph.BerkeleyStorageSetup.getBerkeleyJEConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class BerkeleyLuceneTest extends TitanIndexTest {

    public BerkeleyLuceneTest() {
        super(true, true, true);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = getBerkeleyJEConfiguration();
        //Add index
        config.set(INDEX_BACKEND,"lucene",INDEX);
        config.set(INDEX_DIRECTORY, StorageSetup.getHomeDir("lucene"),INDEX);
        return config.getConfiguration();
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return false;
        //TODO: The query [v.name:"Uncle Berry has a farm"] has an empty result set which indicates that exact string
        //matching inside this query is not supported for some reason. INVESTIGATE!
    }

    @Override
    public boolean supportsWildcardQuery() {
        return false;
    }

    @Override
    protected boolean supportsCollections() {
        return false;
    }
}
