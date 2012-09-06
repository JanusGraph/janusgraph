package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraDaemonWrapper;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Graph;
import org.junit.BeforeClass;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class InternalCassandraBlueprintsTest extends LocalBlueprintsTest {

    private static boolean isStartedUp = false;

    @Override
    public synchronized void startUp() {
        if (!isStartedUp) {
            CassandraDaemonWrapper.start(StorageSetup.cassandraYamlPath);
            isStartedUp=true;
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

    @Override
    public void cleanUp() throws StorageException {
        CassandraThriftStorageManager s = new CassandraThriftStorageManager(
                StorageSetup.getCassandraGraphConfiguration().subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE));
        s.clearStorage();
    }


}
