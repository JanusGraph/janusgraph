package org.janusgraph.graphdb.berkeleyje;

import org.janusgraph.BerkeleyStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusOperationCountingTest;
import org.janusgraph.graphdb.JanusPartitionGraphTest;
import org.junit.Ignore;



public class BerkeleyPartitionGraphTest extends JanusPartitionGraphTest {

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return BerkeleyStorageSetup.getBerkeleyJEGraphConfiguration();
    }

    /**
     * TODO: debug berkeley dbs keyslice method
     */

}
