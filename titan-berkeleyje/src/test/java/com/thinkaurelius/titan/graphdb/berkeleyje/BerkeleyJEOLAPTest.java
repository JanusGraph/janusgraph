package com.thinkaurelius.titan.graphdb.berkeleyje;

import com.thinkaurelius.titan.BerkeleyJeStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import com.thinkaurelius.titan.olap.FulgoraOLAPTest;

public class BerkeleyJEOLAPTest extends FulgoraOLAPTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return BerkeleyJeStorageSetup.getBerkeleyJEGraphConfiguration();
    }

}
