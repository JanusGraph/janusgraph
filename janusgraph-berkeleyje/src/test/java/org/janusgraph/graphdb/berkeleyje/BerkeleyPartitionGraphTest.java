package org.janusgraph.graphdb.berkeleyje;

import org.janusgraph.BerkeleyStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphOperationCountingTest;
import org.janusgraph.graphdb.JanusGraphPartitionGraphTest;
import org.junit.Ignore;



public class BerkeleyPartitionGraphTest extends JanusGraphPartitionGraphTest {

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return BerkeleyStorageSetup.getBerkeleyJEGraphConfiguration();
    }

    /**
     * TODO: debug berkeley dbs keyslice method
     */

}
