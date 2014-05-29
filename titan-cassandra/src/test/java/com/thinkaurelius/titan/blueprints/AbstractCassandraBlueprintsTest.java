package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.embedded.CassandraEmbeddedStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.tinkerpop.blueprints.Graph;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class AbstractCassandraBlueprintsTest extends TitanBlueprintsTest {

    @Override
    public void afterSuite() {
        //Do nothing
    }

    @Override
    public void beforeSuite() {
        //Do nothing
    }

    @Override
    public Graph generateGraph() {
        Graph graph = TitanFactory.open(getGraphConfig());
        return graph;
    }

    @Override
    public void cleanUp() throws StorageException {
        StandardTitanGraph graph = (StandardTitanGraph)generateGraph();
        graph.getConfiguration().getBackend().clearStorage();
        graph.shutdown();
    }

    @Override
    public boolean supportsMultipleGraphs() {
        return false;
    }

    @Override
    public Graph generateGraph(String s) {
        throw new UnsupportedOperationException();
    }

    protected abstract WriteConfiguration getGraphConfig();
}
