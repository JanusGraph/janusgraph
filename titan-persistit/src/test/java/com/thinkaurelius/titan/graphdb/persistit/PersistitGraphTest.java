package com.thinkaurelius.titan.graphdb.persistit;

import com.thinkaurelius.titan.PersistitStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class PersistitGraphTest extends TitanGraphTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return PersistitStorageSetup.getPersistitGraphConfig();
    }

}


