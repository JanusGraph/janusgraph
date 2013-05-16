package com.thinkaurelius.titan.graphdb.berkeleyje;

import com.thinkaurelius.titan.BerkeleyJeStorageSetup;
import com.thinkaurelius.titan.graphdb.SpeedComparisonPerformanceTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.Configuration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BerkeleyJESpeedComparisonPerformanceTest extends SpeedComparisonPerformanceTest {
    
    public BerkeleyJESpeedComparisonPerformanceTest() {
        super(getConfiguration());
    }
    
    private static final Configuration getConfiguration() {
        Configuration config = BerkeleyJeStorageSetup.getBerkeleyJEGraphConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL_KEY,false);
        return config;
    }
}
