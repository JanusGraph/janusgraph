package com.thinkaurelius.titan.graphdb.berkeleyje;

import com.thinkaurelius.titan.BerkeleyStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.SpeedComparisonBenchmark;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BerkeleySpeedComparisonBenchmark extends SpeedComparisonBenchmark {

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = BerkeleyStorageSetup.getBerkeleyJEConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL,false);
        return config.getConfiguration();
    }

}
