package org.janusgraph.graphdb.berkeleyje;

import org.janusgraph.BerkeleyStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphTest;
import org.janusgraph.graphdb.JanusOperationCountingTest;

public class BerkeleyOperationCountingTest extends JanusOperationCountingTest {

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return BerkeleyStorageSetup.getBerkeleyJEGraphConfiguration();
    }

    @Override
    public boolean storeUsesConsistentKeyLocker() {
        return false;
    }

}
