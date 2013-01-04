package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Graph;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class ExternalHBaseBlueprintsTest extends LocalBlueprintsTest {


    @Override
    public void startUp() {
        //Start HBase
    }

    @Override
    public void shutDown() {
        //Stop HBase
    }

    @Override
    public Graph generateGraph() {
        Graph graph = TitanFactory.open(StorageSetup.getHBaseGraphConfiguration());
        return graph;
    }

    // TODO: IT DOES! @Override
    public void cleanUp() throws StorageException {
        HBaseStoreManager s = new HBaseStoreManager(
                StorageSetup.getHBaseGraphConfiguration().subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE));
        s.clearStorage();
    }


}
