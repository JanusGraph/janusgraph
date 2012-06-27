package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Graph;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class ExternalCassandraBlueprintsTest extends LocalBlueprintsTest {

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
        Graph graph = TitanFactory.open(StorageSetup.getCassandraGraphConfiguration());
        return graph;
    }

    @Override
    public void cleanUp() {
        CassandraThriftStorageManager s = new CassandraThriftStorageManager(
                StorageSetup.getCassandraGraphConfiguration().subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE));
        s.clearStorage();
    }


}
