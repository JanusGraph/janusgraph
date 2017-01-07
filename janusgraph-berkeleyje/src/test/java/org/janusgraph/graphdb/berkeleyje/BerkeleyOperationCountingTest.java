package org.janusgraph.graphdb.berkeleyje;

import org.janusgraph.BerkeleyStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphTest;
import org.janusgraph.graphdb.JanusGraphOperationCountingTest;

public class BerkeleyOperationCountingTest extends JanusGraphOperationCountingTest {

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return BerkeleyStorageSetup.getBerkeleyJEGraphConfiguration();
    }

    @Override
    public boolean storeUsesConsistentKeyLocker() {
        return false;
    }

}
