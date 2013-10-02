package com.thinkaurelius.titan.blueprints;

import org.apache.commons.configuration.Configuration;
import org.junit.experimental.categories.Category;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.embedded.CassandraEmbeddedStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Graph;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class EmbeddedCassandraBlueprintsTest extends TitanBlueprintsTest {

    @Override
    public void shutDown() {
        //Do nothing
    }

    @Override
    public void startUp() {
        //Do nothing
    }

    @Override
    public Graph generateGraph() {
        Graph graph = TitanFactory.open(getGraphConfig());
        return graph;
    }

    @Override
    public void cleanUp() throws StorageException {
        CassandraEmbeddedStoreManager s = new CassandraEmbeddedStoreManager(
                getGraphConfig().subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE));
        s.clearStorage();
    }

    @Override
    public boolean supportsMultipleGraphs() {
        return false;
    }

    @Override
    public Graph generateGraph(String s) {
        throw new UnsupportedOperationException();
    }

    private Configuration getGraphConfig() {
        return CassandraStorageSetup.getEmbeddedCassandraGraphConfiguration(getClass().getSimpleName());
    }
}
