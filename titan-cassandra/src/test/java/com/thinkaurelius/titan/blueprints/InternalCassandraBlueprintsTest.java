package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.embedded.CassandraDaemonWrapper;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.CassandraThriftStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Graph;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class InternalCassandraBlueprintsTest extends LocalBlueprintsTest {

    private static boolean isStartedUp = false;

    @Override
    public synchronized void startUp() {
        if (!isStartedUp) {
            CassandraDaemonWrapper.start(StorageSetup.cassandraYamlPath);
            isStartedUp = true;
        }
    }

    @Override
    public void shutDown() {
        //Do nothing
    }

    @Override
    public Graph generateGraph() {
        Graph graph = TitanFactory.open(StorageSetup.getCassandraGraphConfiguration());
        return graph;
    }

    // IT DOES! @Override
    public void cleanUp() throws StorageException {
        CassandraThriftStoreManager s = new CassandraThriftStoreManager(
                StorageSetup.getCassandraGraphConfiguration().subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE));
        s.clearStorage();
    }


}
