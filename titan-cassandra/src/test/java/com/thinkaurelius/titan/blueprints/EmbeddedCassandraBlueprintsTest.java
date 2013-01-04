package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.embedded.CassandraEmbeddedStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Graph;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class EmbeddedCassandraBlueprintsTest extends LocalBlueprintsTest {

    @Override
    public void shutDown() {
        //Do nothing
    }

    @Override
    public Graph generateGraph() {
        Graph graph = TitanFactory.open(StorageSetup.getEmbeddedCassandraGraphConfiguration());
        return graph;
    }

    // TODO: IT DOES! @Override
    public void cleanUp() throws StorageException {
        CassandraEmbeddedStoreManager s = new CassandraEmbeddedStoreManager(
                StorageSetup.getEmbeddedCassandraGraphConfiguration().subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE));
        s.clearStorage();
    }


}
