package com.thinkaurelius.titan.graphdb.persistit;

import com.thinkaurelius.titan.PersistitStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;

public class PersistitGraphConcurrentTest extends TitanGraphConcurrentTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return PersistitStorageSetup.getPersistitGraphConfig();
    }
}
