package com.thinkaurelius.titan.graphdb.persistit;

import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.PersistitStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceMemoryTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class PersistitGraphPerformanceMemoryTest extends TitanGraphPerformanceMemoryTest {

    public PersistitGraphPerformanceMemoryTest() {
        super(getGraphConfig());
    }
    
    private static Configuration getGraphConfig() {
        Configuration base = PersistitStorageSetup.getPersistitGraphConfig();
        base.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).setProperty("buffercount", 128);
        return base;
    }
}
