package com.thinkaurelius.titan.graphdb.berkeleyje;

import com.thinkaurelius.titan.BerkeleyStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;

public class BerkeleyGraphConcurrentTest extends TitanGraphConcurrentTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return BerkeleyStorageSetup.getBerkeleyJEGraphConfiguration();
    }

}
