package com.thinkaurelius.titan.blueprints;

import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.CassandraThriftStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Graph;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ExternalCassandraBlueprintsTest extends TitanBlueprintsTest {

    public static CassandraProcessStarter ch = new CassandraProcessStarter();

    @Override
    public void startUp() {
        ch.startCassandra();
    }

    @Override
    public void shutDown() {
        ch.stopCassandra();
    }

    @Override
    public Graph generateGraph() {
        Graph graph = TitanFactory.open(getGraphConfig());
        return graph;
    }

    @Override
    public void cleanUp() throws StorageException {
        CassandraThriftStoreManager s = new CassandraThriftStoreManager(
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
        return CassandraStorageSetup.getCassandraGraphConfiguration(getClass().getSimpleName());
    }
}
