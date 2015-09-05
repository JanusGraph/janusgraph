package com.thinkaurelius.titan.graphdb.berkeleyje;

import com.thinkaurelius.titan.BerkeleyStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import com.thinkaurelius.titan.graphdb.TitanOperationCountingTest;

public class BerkeleyOperationCountingTest extends TitanOperationCountingTest {

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return BerkeleyStorageSetup.getBerkeleyJEGraphConfiguration();
    }

    @Override
    public boolean storeUsesConsistentKeyLocker() {
        return false;
    }

}
