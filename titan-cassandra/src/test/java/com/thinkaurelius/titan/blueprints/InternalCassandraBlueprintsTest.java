package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.CassandraThriftStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Graph;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class InternalCassandraBlueprintsTest extends TitanBlueprintsTest {

    private static boolean isStartedUp = false;

    @Override
    public synchronized void startUp() {
        if (!isStartedUp) {
            CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.cassandraYamlPath);
            isStartedUp = true;
        }
    }

    @Override
    public void shutDown() {
        //Do nothing
    }

    @Override
    public Graph generateGraph() {
        Graph graph = TitanFactory.open(CassandraStorageSetup.getCassandraGraphConfiguration());
        return graph;
    }

    @Override
    public void cleanUp() throws StorageException {
        CassandraThriftStoreManager s = new CassandraThriftStoreManager(
                CassandraStorageSetup.getCassandraGraphConfiguration().subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE));
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

}
