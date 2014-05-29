package com.thinkaurelius.titan.graphdb.berkeleyje;

import com.thinkaurelius.titan.BerkeleyStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.olap.FulgoraOLAPTest;

public class BerkeleyOLAPTest extends FulgoraOLAPTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return BerkeleyStorageSetup.getBerkeleyJEGraphConfiguration();
    }

}
