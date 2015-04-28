package com.thinkaurelius.titan.hadoop;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;

public class CassandraIndexManagementIT extends AbstractIndexManagementIT {

    @Override
    public WriteConfiguration getConfiguration() {
        String className = getClass().getSimpleName();
        ModifiableConfiguration mc = CassandraStorageSetup.getEmbeddedConfiguration(className);
        return mc.getConfiguration();
    }
}
