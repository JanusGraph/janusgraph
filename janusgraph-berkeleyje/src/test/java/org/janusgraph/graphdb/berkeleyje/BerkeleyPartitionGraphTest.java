package org.janusgraph.graphdb.berkeleyje;

import org.janusgraph.BerkeleyStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.TitanOperationCountingTest;
import org.janusgraph.graphdb.TitanPartitionGraphTest;
import org.junit.Ignore;



public class BerkeleyPartitionGraphTest extends TitanPartitionGraphTest {

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return BerkeleyStorageSetup.getBerkeleyJEGraphConfiguration();
    }

    /**
     * TODO: debug berkeley dbs keyslice method
     */

}
