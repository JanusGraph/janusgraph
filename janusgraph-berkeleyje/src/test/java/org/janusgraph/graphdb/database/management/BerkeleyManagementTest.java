package com.thinkaurelius.titan.graphdb.database.management;

import com.thinkaurelius.titan.BerkeleyStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;

public class BerkeleyManagementTest extends ManagementTest {
    @Override
    public WriteConfiguration getConfiguration() {
        return BerkeleyStorageSetup.getBerkeleyJEGraphConfiguration();
    }
}
