package com.thinkaurelius.titan.graphdb.persistit;

import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.persistit.PersistitStoreManager;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.PersistitStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceMemoryTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class PersistitGraphPerformanceMemoryTest extends TitanGraphPerformanceMemoryTest {

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = PersistitStorageSetup.getPersistitConfig();
        config.set(PersistitStoreManager.BUFFER_COUNT,128);
        return config.getConfiguration();
    }
}
