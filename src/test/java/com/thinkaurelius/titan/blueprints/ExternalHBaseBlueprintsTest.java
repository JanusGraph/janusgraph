package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraLocalhostHelper;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseHelper;
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
        graph = TitanFactory.open(StorageSetup.getHBaseGraphConfiguration());
        return graph;
    }

    @Override
    public void cleanUp() {
        HBaseHelper.deleteAll(StorageSetup.getHBaseGraphConfiguration().subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE));
    }


}
