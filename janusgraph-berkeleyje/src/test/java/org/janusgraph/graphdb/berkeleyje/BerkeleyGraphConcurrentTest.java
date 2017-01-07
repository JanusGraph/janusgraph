package org.janusgraph.graphdb.berkeleyje;

import org.janusgraph.BerkeleyStorageSetup;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphConcurrentTest;

public class BerkeleyGraphConcurrentTest extends JanusGraphConcurrentTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return BerkeleyStorageSetup.getBerkeleyJEGraphConfiguration();
    }

}
