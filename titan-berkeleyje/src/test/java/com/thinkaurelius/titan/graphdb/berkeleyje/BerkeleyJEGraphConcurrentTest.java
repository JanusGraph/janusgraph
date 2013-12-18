package com.thinkaurelius.titan.graphdb.berkeleyje;

import com.thinkaurelius.titan.BerkeleyJeStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;

public class BerkeleyJEGraphConcurrentTest extends TitanGraphConcurrentTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return BerkeleyJeStorageSetup.getBerkeleyJEGraphConfiguration();
    }

}
