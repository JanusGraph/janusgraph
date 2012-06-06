package com.thinkaurelius.titan.graphdb.berkeleyje;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.berkeleydb.je.BerkeleyJEHelper;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class BerkeleyJEGraphTest extends TitanGraphTest {

	public BerkeleyJEGraphTest() {
		super(StorageSetup.getBerkeleyJEGraphConfiguration());
	}

    @Override
    public void cleanUp() {
        BerkeleyJEHelper.clearEnvironment(StorageSetup.getHomeDirFile());
    }
}
