package com.thinkaurelius.titan.graphdb.berkeleyje;

import com.thinkaurelius.titan.BerkeleyStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.SpeedComparisonPerformanceTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.Configuration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BerkeleySpeedComparisonPerformanceTest extends SpeedComparisonPerformanceTest {

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = BerkeleyStorageSetup.getBerkeleyJEConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL,false);
        return config.getConfiguration();
    }

}
